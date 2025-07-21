from enum import Enum


class Constants(Enum):
    path = "/Users/sebastian/Documents/archive"
    name_file = "Questions.csv"
    strategies_order = [
        "Extractor",
        "ChangeType",
        "DropColumns",
        "Lower",
        "PunctuationRemove",
        "StopwordsRemove",
        "FreqwordsRemove",
        "Lemmatize",
        "NumbersRemove",
        "LanguajesRemove",
        "SpacesShortWordsRemove",
        "Loader",
    ]
    format = "csv"
    mode = "overwrite"
    name_out_file = "question_curated"
