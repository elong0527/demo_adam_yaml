# Project Context for Claude

## Important Guidelines
- **Use ASCII characters only** - No emojis or special Unicode characters in code or commits

## Project Overview
This project demonstrates the use of YAML-based specifications for defining ADaM (Analysis Data Model) datasets in clinical trials, following CDISC standards.

## Project Structure
```
demo_adam_yaml/
├── data/                  # CDISC pilot study data in parquet format
│   ├── sdtm/             # SDTM datasets (22 files)
│   └── adam/             # ADaM datasets (10 files)
├── spec/                  # YAML specifications for ADaM datasets
│   ├── adsl_common.yaml  # Common variables across studies
│   ├── adsl_project.yaml # Project-level specifications
│   └── adsl_study.yaml   # Study-specific specifications
├── script/                # Data preparation scripts
│   ├── prepare_data.py   # Downloads and converts CDISC pilot data
│   └── requirements.txt  # Python dependencies
├── ADaM_v2.1/            # ADaM v2.1 documentation (gitignored)
└── ADaMIG/               # ADaM Implementation Guide (gitignored)
```

## Key Technologies
- **Python**: Data processing and transformation
- **Polars**: High-performance dataframe library for data manipulation
- **Parquet**: Columnar storage format for efficient data storage
- **YAML**: Human-readable configuration for ADaM specifications
- **uv**: Fast Python package manager and virtual environment tool

## Data Sources
- CDISC Pilot Study data from: https://github.com/cdisc-org/sdtm-adam-pilot-project
- SDTM datasets: 22 domains including AE, CM, DM, LB, VS, etc.
- ADaM datasets: 10 analysis datasets including ADSL, ADAE, ADVS, etc.

## Development Setup
```bash
# Create virtual environment with uv
uv venv
source .venv/bin/activate

# Install dependencies
uv pip install -r script/requirements.txt

# Download and prepare data (if needed)
python script/prepare_data.py
```

## YAML Specification Design
The YAML specifications follow a hierarchical inheritance model:
1. **adsl_common.yaml**: Base template with common variables
2. **adsl_project.yaml**: Project-level specifications (inherits from common)
3. **adsl_study.yaml**: Study-specific specifications (inherits from project)

Each specification can:
- Inherit from parent configurations using `config` field
- Override inherited variables
- Add new study/project-specific variables

## Testing and Validation Commands
```bash
# Lint Python code (if configured)
# npm run lint or ruff check

# Type checking (if configured)  
# npm run typecheck or mypy

# Run tests (if configured)
# pytest
```

## Important Notes
- All data files are stored in parquet format for efficient storage and processing
- The project uses CDISC pilot study data for demonstration purposes
- YAML specifications support inheritance for DRY (Don't Repeat Yourself) principle
- Virtual environment should be activated before running any Python scripts

## Next Steps and TODOs
- [ ] Implement YAML parser to read and validate specifications
- [ ] Create ADaM dataset generator from YAML specifications
- [ ] Add validation against ADaM conformance rules
- [ ] Generate define.xml from YAML specifications
- [ ] Add comprehensive test suite
- [ ] Create documentation for YAML specification format

## Common Tasks
1. **Update data**: Run `python script/prepare_data.py`
2. **Add new YAML spec**: Create in `spec/` directory following inheritance model
3. **Test changes**: Ensure virtual environment is active and run validation scripts

## References
- [CDISC ADaM v2.1](https://www.cdisc.org/standards/foundational/adam)
- [CDISC Pilot Project](https://github.com/cdisc-org/sdtm-adam-pilot-project)
- [ADaM Implementation Guide](https://www.cdisc.org/standards/foundational/adam/adam-implementation-guide-v1-3)