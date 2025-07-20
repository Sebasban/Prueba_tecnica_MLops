# from extra.utils.extract.extract import Extractor
# from extra.utils.transforms.change_type import ChangeType
# from extra.utils.transforms.freqwords_remove import FreqwordsRemove
# from extra.utils.transforms.languajes_remove import LanguajesRemove
# from extra.utils.transforms.lemmatize import Lemmatize
# from extra.utils.transforms.lower import Lower
# from extra.utils.transforms.numbers_remove import NumbersRemove
# from extra.utils.transforms.punctuation_remove import PunctuationRemove
# from extra.utils.transforms.spaces_short_words_remove import SpacesShortWordsrRemove
# from extra.utils.transforms.stopwords_remove import StopwordsrRemove
# from extra.utils.transforms.drop_columns import DropColumns
# from extra.utils.load.load import Loader
from ..extract.extract import Extractor
from ..transforms.change_type import ChangeType
from ..transforms.freqwords_remove import FreqwordsRemove
from ..transforms.languajes_remove import LanguajesRemove
from ..transforms.lemmatize import Lemmatize
from ..transforms.lower import Lower
from ..transforms.numbers_remove import NumbersRemove
from ..transforms.punctuation_remove import PunctuationRemove
from ..transforms.spaces_short_words_remove import SpacesShortWordsrRemove
from ..transforms.stopwords_remove import StopwordsrRemove
from ..transforms.drop_columns import DropColumns
from ..load.load import Loader


class Factory:
    @staticmethod
    def factory(name: str):
        strategies = {
            "DropColumns": DropColumns,
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
            "Loader": Loader,
        }
        if name not in strategies:
            raise ValueError(f"Unknow transform: {name}")
        else:
            return strategies[name]()
