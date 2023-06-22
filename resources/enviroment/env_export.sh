#!/usr/bin/env sh

echo "[!] Exporting to requirements.txt . . ."
pip list --format=freeze > requirements.txt

echo "[!] Exporting to enviroment.yml . . ."
conda env export > environment.yml

echo "[+] Envrioment exported!"
