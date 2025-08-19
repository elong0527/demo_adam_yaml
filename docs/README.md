# Documentation

This folder contains the documentation for the ADaM YAML project, built with Quarto and featuring interactive tables using reactable.

## Local Development

### Prerequisites

1. Install Quarto: https://quarto.org/docs/get-started/
2. Install Python dependencies:
   ```bash
   uv pip install -r docs/requirements.txt
   ```

### Building Locally

To build the documentation locally:

```bash
cd docs
quarto render --to html
```

The built site will be in `docs/_site/`.

### Preview Locally

To preview the documentation with live reload:

```bash
cd docs
quarto preview
```

This will open the documentation in your browser at http://localhost:4000.

## GitHub Actions Deployment

The documentation is automatically built and deployed to GitHub Pages when changes are pushed to the main branch.

### Setup GitHub Pages

1. Go to your repository settings on GitHub
2. Navigate to "Pages" in the sidebar
3. Under "Build and deployment":
   - Source: Select "GitHub Actions"
4. The workflow will automatically deploy to: https://[username].github.io/demo_adam_yaml/

### Workflow

The `.github/workflows/docs.yml` workflow:
1. Builds the documentation using Quarto
2. Deploys to GitHub Pages (main branch only)

## Documentation Structure

- `index.qmd` - Homepage with overview and quick start
- `example_adsl.qmd` - Interactive ADSL dataset example with reactable tables
- `api.qmd` - API reference documentation
- `_quarto.yml` - Quarto configuration
- `styles.css` - Custom styling
- `requirements.txt` - Python dependencies

## Adding New Pages

1. Create a new `.qmd` file in the docs folder
2. Add it to the navigation in `_quarto.yml`
3. Use Python code blocks with `{python}` for executable code
4. Use reactable for interactive tables