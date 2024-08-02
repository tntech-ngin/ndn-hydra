# How to upgrade package version

---

1. After doing all work for the new version, _open `docs/version.py`_ and put the new version there
2. Then do you **last commit** with the message `Bump version to X.X.X` with the following version (e.g 0.3.35, `Bump version to 0.3.35`)
3. Create a pull request
4. After revision from other team's member, merge the code with the new version

The workflow will recognize the new version, build the package, deploy it into PyPi and TestPypi, and finally publish the new version on GitHub.

