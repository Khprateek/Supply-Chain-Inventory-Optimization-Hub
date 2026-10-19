# Contributing to Enterprise Open Data Lakehouse Platform

Thank you for your interest in contributing to the **Enterprise Open Data Lakehouse Platform**! We welcome bug reports, feature enhancements, documentation improvements, and architectural suggestions.

Please take a few moments to review these guidelines before submitting a contribution.

---

## 📜 Code of Conduct

All contributors and maintainers are expected to adhere to our [Code of Conduct](CODE_OF_CONDUCT.md). Please treat all participants with respect and professionalism.

---

## 🛠️ Development Workflow

### 1. Fork and Clone
```bash
git clone https://github.com/<your-username>/Enterprise-Supply-Chain-Lakehouse.git
cd Enterprise-Supply-Chain-Lakehouse
```

### 2. Set Up Local Python Environment
We require **Python 3.10+** (Python 3.12 recommended).

```bash
# Create virtual environment
python -m venv .venv

# Activate on Windows (PowerShell):
.\.venv\Scripts\Activate.ps1
# Or on Linux / macOS:
source .venv/bin/activate

# Install core and development dependencies
pip install -r requirements.txt
pip install -e ".[dev]"
```

### 3. Start Docker Infrastructure
```bash
docker compose up -d
```
Verify containers (`kafka`, `zookeeper`, `spark-master`, `s3`, `nessie`, `trino`) are healthy:
```bash
docker ps
```

---

## 🌿 Branching Strategy & Git Conventions

1. **Branch Naming**:
   * Feature: `feat/add-iceberg-partition-spec`
   * Bug Fix: `fix/kafka-buffer-overflow`
   * Documentation: `docs/update-benchmark-spec`
   * Performance: `perf/optimize-spark-shuffle`
   * Maintenance: `chore/bump-dependencies`

2. **Conventional Commits**:
   Please follow the [Conventional Commits](https://www.conventionalcommits.org/) specification:
   ```text
   feat(streaming): support dynamic watermark sliding window
   fix(lakehouse): prevent JVM crash during Trino metadata sync
   docs(readme): add Trino OLAP query samples
   test(benchmark): automate offset watermark verification
   ```

---

## 🧪 Testing and Quality Standards

Before opening a pull request, ensure all tests pass:

```bash
# Run unit & schema validation suite
python tests/validate_dataset.py

# Run engineering benchmark suite (Test 1 through 5)
python tests/benchmark_suite.py --test all

# Lint with Ruff
ruff check .

# Format with Black
black --check .
```

---

## 📬 Submitting a Pull Request (PR)

1. **Push your branch** to your fork:
   ```bash
   git push origin feat/your-feature-name
   ```
2. **Open a Pull Request** against the `main` branch.
3. Fill out the **Pull Request Template**:
   * Explain the *why* behind the change.
   * Provide reproduction steps or benchmark verification numbers.
   * Attach relevant screenshots or CLI log outputs if applicable.
4. Ensure all **CI checks** pass. Maintainers will review your PR and provide constructive feedback.

---

## 💡 Reporting Issues

* **Bug Reports**: Use the [Bug Report template](.github/ISSUE_TEMPLATE/bug_report.yml). Include Docker version, OS, logs, and reproducible steps.
* **Feature Requests**: Use the [Feature Request template](.github/ISSUE_TEMPLATE/feature_request.yml) to outline the problem and proposed solution.
