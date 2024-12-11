# ## Python StdLib Imports ----
from dataclasses import dataclass, field
from typing import List, Literal, Optional, Tuple, Union


@dataclass
class WhitespaceChatacter:
    name: str
    unicode: str
    ascii: int
    chr: str = field(init=False)

    def __post_init__(self) -> None:
        self.chr = chr(self.ascii)

    def __getitem__(
        self, attribute: Literal["name", "unicode", "ascii", "chr"]
    ) -> Union[str, int]:
        return getattr(self, attribute)


@dataclass
class WhitespaceCharacters:
    characters: Union[
        List[WhitespaceChatacter],
        Tuple[WhitespaceChatacter],
    ]

    def __iter__(self):
        yield from self.characters

    def to_list(
        self,
        attr: Optional[Literal["name", "unicode", "ascii", "chr"]] = None,
    ) -> List[Union[str, int, WhitespaceChatacter]]:
        return [char if attr is None else char[attr] for char in self]
