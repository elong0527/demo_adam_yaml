"""
Data models for ADaM YAML specifications
"""

from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Union
from enum import Enum


class CoreType(str, Enum):
    """ADaM variable core types"""
    CDISC_REQUIRED = "cdisc-required"
    COMPANY_REQUIRED = "company-required"
    OPTIONAL = "optional"
    CONDITIONAL = "conditional"


class DataType(str, Enum):
    """Supported data types for ADaM variables"""
    STR = "str"
    INT = "int"
    FLOAT = "float"
    DATE = "date"
    DATETIME = "datetime"
    BOOLEAN = "bool"


@dataclass
class ValidationRule:
    """Validation rules for a column"""
    maximum_missing_percentage: Optional[float] = None
    allowed_values: Optional[List[Any]] = None
    min: Optional[Union[int, float]] = None
    max: Optional[Union[int, float]] = None
    unique: Optional[bool] = None
    pattern: Optional[str] = None
    not_null: Optional[bool] = None
    length: Optional[Dict[str, int]] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary, excluding None values"""
        return {k: v for k, v in self.__dict__.items() if v is not None}


@dataclass
class Derivation:
    """Derivation rules for a column"""
    source: Optional[str] = None
    constant: Optional[Any] = None
    filter: Optional[str] = None
    function: Optional[str] = None
    height: Optional[str] = None
    weight: Optional[str] = None
    join: Optional[Dict] = None
    expression: Optional[str] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary, excluding None values"""
        return {k: v for k, v in self.__dict__.items() if v is not None}


@dataclass
class Column:
    """Represents an ADaM column specification"""
    name: str
    label: Optional[str] = None
    type: Optional[str] = None
    derivation: Optional[Derivation] = None
    validation: Optional[ValidationRule] = None
    core: Optional[str] = None
    drop: Optional[bool] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary format"""
        result = {"name": self.name}
        if self.label:
            result["label"] = self.label
        if self.type:
            result["type"] = self.type
        if self.derivation:
            result["derivation"] = self.derivation.to_dict()
        if self.validation:
            result["validation"] = self.validation.to_dict()
        if self.core:
            result["core"] = self.core
        if self.drop:
            result["drop"] = self.drop
        return result
    
    @classmethod
    def from_dict(cls, data: Dict) -> "Column":
        """Create Column from dictionary"""
        derivation = None
        if "derivation" in data:
            derivation = Derivation(**data["derivation"])
        
        validation = None
        if "validation" in data:
            validation = ValidationRule(**data["validation"])
        
        return cls(
            name=data["name"],
            label=data.get("label"),
            type=data.get("type"),
            derivation=derivation,
            validation=validation,
            core=data.get("core"),
            drop=data.get("drop")
        )


@dataclass
class ADaMSpec:
    """Complete ADaM specification"""
    domain: str = "ADSL"
    key: List[str] = field(default_factory=list)
    columns: List[Column] = field(default_factory=list)
    config: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary format"""
        result = {
            "domain": self.domain,
            "key": self.key,
            "columns": [col.to_dict() for col in self.columns]
        }
        if self.config:
            result["config"] = self.config
        return result