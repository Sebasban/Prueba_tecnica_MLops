import sys
import os
import inspect
import pytest
# Tests for Factory
from PruebaTecnicaMLopsRiesgos.extra.utils.factory.factory import Factory
from PruebaTecnicaMLopsRiesgos.extra.utils.extract.extract import Extractor
from PruebaTecnicaMLopsRiesgos.extra.utils.transforms.change_type import ChangeType
from PruebaTecnicaMLopsRiesgos.extra.utils.transforms.freqwords_remove import (
    FreqwordsRemove,
)
from PruebaTecnicaMLopsRiesgos.extra.utils.transforms.languajes_remove import (
    LanguajesRemove,
)
from PruebaTecnicaMLopsRiesgos.extra.utils.transforms.lemmatize import Lemmatize
from PruebaTecnicaMLopsRiesgos.extra.utils.transforms.lower import Lower
from PruebaTecnicaMLopsRiesgos.extra.utils.transforms.numbers_remove import (
    NumbersRemove,
)
from PruebaTecnicaMLopsRiesgos.extra.utils.transforms.punctuation_remove import (
    PunctuationRemove,
)
from PruebaTecnicaMLopsRiesgos.extra.utils.transforms.spaces_short_words_remove import (
    SpacesShortWordsrRemove,
)
from PruebaTecnicaMLopsRiesgos.extra.utils.transforms.stopwords_remove import (
    StopwordsrRemove,
)
from PruebaTecnicaMLopsRiesgos.extra.utils.transforms.drop_columns import DropColumns
from PruebaTecnicaMLopsRiesgos.extra.utils.load.load import Loader
# Tests for Constants enum
from PruebaTecnicaMLopsRiesgos.extra.utils.constants.constants import Constants
# Tests for loader
from PruebaTecnicaMLopsRiesgos.extra.utils.load.load import Loader
# Tests for Extractor
from PruebaTecnicaMLopsRiesgos.extra.utils.extract.extract import Extractor

# Tests for interface abstract base classes
from PruebaTecnicaMLopsRiesgos.extra.utils.interfaces_strategies.extract_strategy_interface import (
    ExtractStrategy,
)
from PruebaTecnicaMLopsRiesgos.extra.utils.interfaces_strategies.transform_strategy_interface import (
    TransformStrategy,
)

# Ensure the project root is on sys.path
current_dir = os.path.dirname(__file__)
repo_root = os.path.abspath(os.path.join(current_dir, ".."))
sys.path.insert(0, repo_root)




def test_constants_values():
    # Check basic types and values
    assert isinstance(Constants.path.value, str)
    assert Constants.name_file.value == "Questions.csv"
    assert isinstance(Constants.strategies_order.value, list)
    assert "Extractor" in Constants.strategies_order.value
    assert Constants.format.value == "csv"
    assert Constants.mode.value == "overwrite"
    assert Constants.name_out_file.value == "question_curated"



valid_strategies = {
    "Extractor": Extractor,
    "ChangeType": ChangeType,
    "FreqwordsRemove": FreqwordsRemove,
    "LanguajesRemove": LanguajesRemove,
    "Lemmatize": Lemmatize,
    "Lower": Lower,
    "NumbersRemove": NumbersRemove,
    "PunctuationRemove": PunctuationRemove,
    "SpacesShortWordsRemove": SpacesShortWordsrRemove,
    "StopwordsRemove": StopwordsrRemove,
    "DropColumns": DropColumns,
    "Loader": Loader,
}


def test_factory_valid_strategies():
    for name, cls in valid_strategies.items():
        instance = Factory.factory(name)
        assert isinstance(
            instance, cls
        ), f"Factory should create instance of {cls} for key '{name}'"


def test_factory_invalid_strategy():
    with pytest.raises(ValueError):
        Factory.factory("InvalidStrategy")




def test_extract_strategy_is_abstract():
    with pytest.raises(TypeError):
        ExtractStrategy()


def test_transform_strategy_is_abstract():
    with pytest.raises(TypeError):
        TransformStrategy()





class FakeDF:
    def __init__(self):
        self.drop_called = None

    def drop(self, col):
        self.drop_called = col
        return self


class FakeReader:
    pass


class FakeSpark:
    def __init__(self):
        self.read = FakeReader()

        # Attach csv method dynamically
        def csv(
            path,
            header,
            inferSchema,
            sep,
            dateFormat,
            nullValue,
            multiLine,
            quote,
            escape,
        ):
            # store call for inspection
            FakeReader.last_call = dict(
                path=path,
                header=header,
                inferSchema=inferSchema,
                sep=sep,
                dateFormat=dateFormat,
                nullValue=nullValue,
                multiLine=multiLine,
                quote=quote,
                escape=escape,
            )
            return FakeDF()

        self.read.csv = csv


def test_extractor_extract_reads_and_drops():
    extractor = Extractor()
    fake_spark = FakeSpark()
    path = "/some/path"
    name_file = "data.csv"
    df = extractor.extract(fake_spark, path, name_file, encoding="utf-8")
    # Check that csv was called with correct arguments
    call = FakeReader.last_call
    assert call["path"] == f"{path}/{name_file}"
    assert call["header"] is True
    assert call["inferSchema"] is True
    assert call["sep"] == ","
    # Check drop was called on returned DataFrame
    assert isinstance(df, FakeDF)
    assert df.drop_called == "Unnamed: 0"





class DummyWriter:
    def __init__(self):
        self.calls = []

    def format(self, fmt):
        self.calls.append(("format", fmt))
        return self

    def option(self, key, value):
        self.calls.append(("option", key, value))
        return self

    def mode(self, mode):
        self.calls.append(("mode", mode))
        return self

    def save(self, path):
        self.calls.append(("save", path))
        return self


class FakeDFLoader:
    def __init__(self):
        self.write = DummyWriter()

    def coalesce(self, num):
        # reset writer for each call
        self.write = DummyWriter()
        return self


def test_loader_load_invokes_write_chain(tmp_path):
    df = FakeDFLoader()
    loader = Loader()
    fmt = "csv"
    mode = "overwrite"
    path = str(tmp_path)
    name_file = "out"
    loader.load(df, fmt, mode, path, name_file)
    calls = df.write.calls
    # Validate sequence of calls
    assert ("format", fmt) in calls
    assert ("option", "header", "true") in calls
    assert ("mode", mode) in calls
    assert ("save", f"{path}/{name_file}") in calls


# Tests for transform strategy signatures
transform_signature_tests = [
    (ChangeType, 3),
    (DropColumns, 2),
    (Lower, 2),
    (PunctuationRemove, 2),
    (Lemmatize, 2),
    (NumbersRemove, 2),
    (LanguajesRemove, 2),
    (SpacesShortWordsrRemove, 2),
    (StopwordsrRemove, 3),
    (FreqwordsRemove, 3),
]


@pytest.mark.parametrize("cls,param_count", transform_signature_tests)
def test_transform_method_signature_counts(cls, param_count):
    instance = cls()
    sig = inspect.signature(instance.transform)
    # parameter count includes 'self'
    assert (
        len(sig.parameters) == param_count
    ), f"{cls.__name__}.transform should have {param_count} parameters"
    assert "df" in sig.parameters
    if param_count == 3:
        assert "spark" in sig.parameters
