"""
Implements indexing logic
and registers :term:`indexable sources <indexable source>`
of bibliographic data
based on the list in :data:`bibxml.settings.RELATON_DATASETS`
using :func:`sources.indexable.register_git_source()`.

Each of the repositories is expected to follow certain structure
with Relaton bibliographic item data serialized to YAML files
under ``/data/`` directory under repository root
(see :func:`.index_dataset()` for indexing logic).

.. seealso:: :rfp:req:`3`
"""
from typing import Tuple, List, Dict, Any, Set, cast
import glob
from os import path
import datetime

import yaml
from celery.utils.log import get_task_logger
from relaton.models import dates, BibliographicItem
from pydantic import ValidationError
from django.core.exceptions import ImproperlyConfigured
from django.conf import settings
from django.db import transaction

from bib_models.util import normalize_relaxed
from common.util import as_list
from common.pydantic import ValidationErrorDict, pretty_print_loc
from common.pydantic import unpack_dataclasses
from sources import indexable

from .types import IndexedSourceMeta, IndexedObject, IndexingOutcome
from .models import RefData


logger = get_task_logger(__name__)


# Below guards against possible AttributeError during setting access
# and performs runtime sanity check that requisite settings are given.

DATASETS = getattr(
    settings,
    'RELATON_DATASETS',
    [])

if len(DATASETS) < 1:
    raise ImproperlyConfigured("No Relaton datasets configured")

DATASET_SOURCE_OVERRIDES = getattr(
    settings,
    'DATASET_SOURCE_OVERRIDES',
    {})

DEFAULT_DATASET_REPO_URL_TEMPLATE = cast(str, getattr(
    settings,
    'DEFAULT_DATASET_REPO_URL_TEMPLATE',
    None))

DEFAULT_DATASET_REPO_BRANCH = getattr(
    settings,
    'DEFAULT_DATASET_REPO_BRANCH',
    None)

have_explicit_sources_for_all_datasets = all([
    dataset_id in DATASET_SOURCE_OVERRIDES
    for dataset_id in DATASETS
])

have_fallback_defaults = all([
    DEFAULT_DATASET_REPO_BRANCH,
    DEFAULT_DATASET_REPO_URL_TEMPLATE,
])

if not have_explicit_sources_for_all_datasets and not have_fallback_defaults:
    raise ImproperlyConfigured(
        "Missing Relaton source Git repository defaults, "
        "and not all sources have their locations explicitly specified")


# Repository discovery
# ====================

get_github_web_data_root = (
    lambda repo_home, branch:
    f'{repo_home}/tree/{branch}'
)
get_github_web_data_root_at_version = (
    lambda repo_home, version:
    f'{repo_home}/blob/{version}'
)
get_github_web_issues = (
    lambda repo_home:
    f'{repo_home}/issues/new'
)


def noop(*args, **kwargs):
    pass


def get_source_meta(ref: RefData) -> IndexedSourceMeta:
    repo_home, _ = locate_relaton_source_repo(ref.dataset)
    repo_name = repo_home.split('/')[-1]
    repo_issues = get_github_web_issues(repo_home)

    return IndexedSourceMeta(
        id=repo_name,
        version=ref.dataset_version,
        home_url=repo_home,
        issues_url=repo_issues,
    )


def get_indexed_object_meta(ref: RefData) -> IndexedObject:
    repo_home, branch = locate_relaton_source_repo(ref.dataset)
    ver = ref.dataset_version
    if ver := ref.dataset_version:
        file_url = (
            f'{get_github_web_data_root_at_version(repo_home, ver)}'
            f'/data/{ref.ref}.yaml'
        )
    else:
        file_url = (
            f'{get_github_web_data_root(repo_home, branch)}'
            f'/data/{ref.ref}.yaml'
        )

    return IndexedObject(
        name=ref.ref,
        external_url=file_url,
        indexed_at=ref.indexed_at,
    )


def locate_relaton_source_repo(dataset_id: str) -> Tuple[str, str]:
    """
    Given a Relaton dataset ID, returns Git repository information
    (URL and branch) using :data:`bibxml.settings.DATASET_SOURCE_OVERRIDES`
    with fallbacks to :data:`bibxml.settings.DEFAULT_DATASET_REPO_URL_TEMPLATE`
    and :data:`bibxml.settings.DEFAULT_DATASET_REPO_BRANCH`.

    .. important:: Does not verify that repository and branch do in fact exist;
                   ensuring that settings reference correct repositories
                   is considered a responsibility of operations engineers.

    :param dataset_id: dataset ID as string
    :returns: tuple (repo_url, repo_branch)
    """
    overrides = (DATASET_SOURCE_OVERRIDES.
                 get(dataset_id, {}).
                 get('relaton_data', {}))

    return (
        overrides.get(
            'repo_url',
            DEFAULT_DATASET_REPO_URL_TEMPLATE.format(dataset_id=dataset_id)),
        overrides.get(
            'repo_branch',
            DEFAULT_DATASET_REPO_BRANCH),
    )


# Source registration
# ===================

def register_relaton_source(source_id: str):
    indexable.register_git_source(
        source_id,
        [
            locate_relaton_source_repo(source_id),
        ],
    )({
        'indexer': (
            lambda ds_ver, dirs, refs, on_progress, on_error:
            index_dataset(
                source_id,
                ds_ver,
                path.join(dirs[0], 'data'),
                refs,
                on_progress,
                on_error,
            )
        ),
        'reset_index': (lambda: reset_index_for_dataset(source_id)),
        'count_indexed': (
            lambda: RefData.objects.filter(dataset=source_id).count()
        ),
    })


for source_id in settings.RELATON_DATASETS:
    register_relaton_source(source_id)


# Indexing implementation
# =======================

def index_dataset(ds_id: str, ds_version: str, relaton_path: str,
                  refs=None,
                  on_progress=None,
                  on_error=None) -> Tuple[int, int]:
    """Indexes Relaton data into :class:`~.models.RefData` instances.

    :param ds_id: dataset ID as a string
    :param relaton_path: path to Relaton source files

    :param refs: a list of string refs to index, or nothing to index everything
    :param on_progress: progress report lambda taking two ints (total, indexed)

    :returns: a tuple of two integers (total, indexed)

    :raise EnvironmentError: passes through any IOError, FileNotFoundError etc.
    """
    yaml.SafeLoader.yaml_implicit_resolvers = {
        k: [r for r in v if r[0] != "tag:yaml.org,2002:timestamp"]
        for k, v in yaml.SafeLoader.yaml_implicit_resolvers.items()
    }

    report_progress = on_progress or noop

    indexing_subset = refs is not None

    requested_refs = set(refs or [])

    relaton_source_files = glob.glob("%s/*.yaml" % relaton_path)

    index_ts = datetime.datetime.now()

    on_error = on_error or noop

    refs_with_paths: List[Tuple[str, str]] = [
        (ref, fpath)
        for fpath in relaton_source_files
        if ((ref := path.splitext(path.basename(fpath))[0])
            and path.isfile(fpath))
    ]

    total = len(refs_with_paths)

    if total < 1:
        raise RuntimeError("Nothing to index")

    report_progress(total, 0)

    indexed_refs: Set[str] = set()

    # Does the indexing. Can be wrapped in a transaction, or not.
    # Call this function only once.
    def index_requested(refs_with_paths: List[Tuple[str, str]]):
        for idx, (ref, relaton_fpath) in enumerate(refs_with_paths):
            report_progress(total, idx)

            with open(relaton_fpath, 'r', encoding='utf-8') \
                 as relaton_fhandler:
                ref_data = yaml.load(
                    relaton_fhandler.read(),
                    Loader=yaml.SafeLoader)

                latest_date = max(
                    to_dates(as_list(ref_data.get('date', [])))
                    or [index_ts.date()]
                )

                outcome = IndexingOutcome(
                    success=False,
                    num_validation_errors=0,
                    validation_errors=[],
                )

                try:
                    bibitem = BibliographicItem(**ref_data)

                except ValidationError as validation_error:
                    errs = cast(
                        List[ValidationErrorDict],
                        validation_error.errors(),
                    )
                    err_desc = '\n'.join([
                        f"{d['type']} at "
                        f"{pretty_print_loc(d['loc'])}: {d['msg']}"
                        for d in errs
                    ])
                    outcome['validation_errors'] = errs
                    outcome['num_validation_errors'] = len(errs)
                    try:
                        normalize_relaxed(ref_data)
                    except Exception as exc:
                        on_error(
                            ref,
                            "Error during normalization:\n%s"
                            % str(exc))
                    try:
                        bibitem = BibliographicItem(**ref_data)
                    except ValidationError:
                        bibitem = None
                        on_error(
                            ref,
                            'Errors not resolved (item skipped):\n%s'
                            % err_desc)
                    # except Exception as exc:
                    #     on_error(
                    #         ref,
                    #         'Errors not resolved '
                    #         '(failed with %s):\n%s'
                    #         % (str(exc), err_desc))
                    else:
                        # on_error(
                        #     ref,
                        #     'Loose data (had to be normalized):\n%s'
                        #     % err_desc)
                        outcome['success'] = True
                else:
                    outcome['success'] = True

                if bibitem:
                    RefData.objects.update_or_create(
                        ref=ref,
                        dataset=ds_id,
                        defaults=dict(
                            # NOTE: moving dataset_version outside ``defaults``
                            # allows maintaining indexed items
                            # for previously indexed dataset versions
                            # but requires more complex lookups
                            # and periodic cleanup of old versions
                            dataset_version=ds_version,
                            indexed_at=index_ts,
                            indexing_outcome=outcome,

                            body=unpack_dataclasses(bibitem.dict())
                            if bibitem
                            else ref_data,
                            latest_date=latest_date,
                            representations=dict(),
                        ),
                    )

                    indexed_refs.add(ref)

    # Partial index is wrapped in a transaction,
    # we don’t index anything if a single item fails.
    if indexing_subset:
        with transaction.atomic():
            index_requested([
                (ref, fpath)
                for ref, fpath in refs_with_paths
                if ref in requested_refs
            ])

            # If we’re indexing a subset of refs,
            # and some of those refs were not found in source,
            # delete those refs from the dataset.
            missing_refs = requested_refs - indexed_refs
            (RefData.objects.
                filter(dataset=ds_id, ref__in=missing_refs).
                delete())

    # Full index is done in batches and one item failing
    # means preceding items may still be indexed.
    else:
        index_requested(refs_with_paths)

        # If we’re reindexing the entire dataset,
        # delete all refs not found in source at the end.
        (RefData.objects.
            filter(dataset=ds_id).
            exclude(ref__in=[ref for (ref, fpath) in refs_with_paths]).
            delete())

    return total, len(indexed_refs)


def to_dates(items: List[Dict[str, Any]]) -> List[datetime.date]:
    """Converts a list of dates in raw deserialized Relaton data
    into a list of ``datetime.date`` objects."""

    result: List[datetime.date] = []
    for item in items:
        raw_date = item.get('value', None)
        if raw_date:
            parsed = dates.parse_date_pydantic(raw_date)
            if parsed:
                result.append(parsed)
            else:
                relaxed = dates.parse_relaxed_date(raw_date)
                if relaxed is not None:
                    result.append(relaxed[0])
    return result


def reset_index_for_dataset(ds_id):
    """Deletes all references for given dataset."""

    with transaction.atomic():
        (RefData.objects.
            filter(dataset=ds_id).
            delete())
