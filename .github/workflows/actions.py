name: run actions.py

on:
  schedule:
    - cron: '0 0 * * 1' # At 00:00 on Monday

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - name: checkout repo content
        uses: actions/checkout@v2 # checkout the repository content

      - name: setup python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10' # install the python version needed

      - name: install python packages
        run: |
          python -m pip install --upgrade pip

      - name: execute py script # run main.py
        run: python actions.py
