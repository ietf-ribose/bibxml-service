from unittest import TestCase

from bib_models import DocID

from ..util import get_primary_docid


class UtilTestcase(TestCase):
    """
    Test cases for units in util.py
    """

    def test_get_primary_docid(self):
        primary_id_value = "primary_id_value"
        raw_ids = [
            DocID(id=primary_id_value, type="type", primary= True),
            DocID(id="id2", type="type2"),
            DocID(id="id3", type="type3", scope="scope"),
        ]
        primary_id = get_primary_docid(raw_ids)
        self.assertIsNotNone(primary_id)
        self.assertIsInstance(primary_id, DocID)
        self.assertEqual(primary_id.id, primary_id_value)  # type: ignore

    def test_fail_get_primary_docid_if_no_primary_id(self):
        """
        get_primary_docid should return None if no entry has primary == True
        """
        primary_id_value = "primary_id_value"
        raw_ids = [
            DocID(id=primary_id_value, type="type", primary=False),
            DocID(id="id2", type="type2"),
            DocID(id="id3", type="type3", scope="scope"),
        ]
        self.assertIsNone(get_primary_docid(raw_ids))
