# app/parsers/base.py

from abc import ABC, abstractmethod
from typing import Dict, List

class BaseParser(ABC):
    @abstractmethod
    def sniff(self, path: str) -> Dict:
        """
        Retorna metadatos + errores/warnings si el archivo no cuadra con el formato.
        Debe ser rápido (no parse completo).
        """
        raise NotImplementedError

    @abstractmethod
    def parse(self, path: str) -> List[dict]:
        """
        Retorna lista de dicts normalizados.
        """
        raise NotImplementedError