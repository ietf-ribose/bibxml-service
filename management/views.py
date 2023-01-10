"""View functions for management GUI."""

from typing import List

from django.shortcuts import render
from django.conf import settings
from django.http.request import split_domain_port
from django.http import HttpResponseNotFound

from sources.task_status import get_dataset_task_history
from sources.task_status import describe_indexing_task
from sources.task_status import list_running_tasks
from sources.task_status import get_indexable_source_status
from sources.task_status import IndexableSourceStatus
from sources import indexable


shared_context = dict(
    # NOTE: Use this context only in auth-guarded views
    api_secret=settings.API_SECRETS[0],
)


def home(request):
    running_tasks = [
        describe_indexing_task(tid)
        for tid in list_running_tasks()
    ]

    return render(request, 'management/home.html', dict(
        **shared_context,
        running_tasks=running_tasks,
        task_monitor_host="{}:{}".format(
            split_domain_port(request.get_host())[0],
            5555),
    ))


def datasets(request):
    """Indexable sources."""

    sources: List[IndexableSourceStatus] = []
    for source in indexable.registry.values():
        # TODO: Annotate/aggregate indexed item counts in management GUI?
        sources.append(get_indexable_source_status(source))

    return render(request, 'management/datasets.html', dict(
        **shared_context,
        datasets=sources,
    ))


def dataset(request, dataset_id: str):
    """:term:`indexable source` indexing history & running tasks."""

    try:
        source = indexable.registry[dataset_id]
    except KeyError:
        return HttpResponseNotFound(
            f"Source {dataset_id} not found".encode('utf-8'))

    return render(request, 'management/dataset.html', dict(
        **shared_context,
        dataset_id=dataset_id,
        source=source,
        history=get_dataset_task_history(dataset_id),
    ))


def indexing_task(request, task_id: str):
    """Indexing task run for an indexable source."""

    dataset_id = request.GET.get('dataset_id', None)
    if dataset_id:
        try:
            source = indexable.registry[dataset_id]
        except KeyError:
            source = None
    else:
        source = None

    return render(request, 'management/task.html', dict(
        **shared_context,
        source=source,
        task=describe_indexing_task(task_id),
    ))
