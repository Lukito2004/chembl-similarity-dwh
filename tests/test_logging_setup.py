import logging

from chembl_sim import logging_setup


def test_a_handler_is_attached_when_none_exists(monkeypatch):
    root = logging.getLogger()
    monkeypatch.setattr(root, "handlers", [])
    logging_setup.configure_logging("DEBUG")
    assert root.handlers
    assert root.level == logging.DEBUG


def test_existing_handlers_are_left_alone(monkeypatch):
    root = logging.getLogger()
    existing = logging.NullHandler()
    monkeypatch.setattr(root, "handlers", [existing])
    logging_setup.configure_logging()
    assert root.handlers == [existing]


def test_a_module_logger_carries_its_name():
    assert logging_setup.get_logger("chembl_sim.test").name == "chembl_sim.test"
