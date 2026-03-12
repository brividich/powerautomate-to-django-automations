# Power Automate To Django Automations

App standalone per analizzare export Power Automate `.zip` / `.json`, evidenziare incongruenze, applicare remediation guidate e generare package di conversione verso il motore automazioni del portale Django.

## Avvio rapido

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python app\webapp.py
```

UI locale:

- `http://127.0.0.1:8787`

## Funzioni

- upload export Power Automate
- analisi trigger, azioni, connettori e campi
- storico conversioni con pagina dettaglio
- remediation automatica per casi `assenze`
- download `automation_package.json`

## Output batch

Per generare gli artefatti da riga di comando:

```powershell
python app\main.py
```

Artefatti prodotti:

- `output/normalized/*.json`
- `output/previews/*.md`
- `output/packages/*.automation_package.json`
- `output/history/*.json`
