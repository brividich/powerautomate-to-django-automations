# CHANGELOG

Questo progetto segue un changelog semplice in stile semver, pensato per un repository GitHub: una sezione `Unreleased` sempre in testa e release datate quando si taglia una versione.

## Unreleased

- Nessuna modifica ancora rilasciata dopo `0.1.0-dev`.

## 0.1.0-dev - 2026-03-12

- **[feature] App standalone di conversione**: aggiunta una UI web locale Flask per caricare export Power Automate `.zip` / `.json`, analizzarli e salvarne lo storico locale.
- **[feature] Dettaglio conversione e storico**: introdotte pagine dedicate con riepilogo tecnico del flow, incompatibilita', mapping campi candidato, regole draft proposte e download del package JSON finale.
- **[feature] Remediation applicabile**: aggiunto il pulsante `Applica Remediation` che aggiorna il package salvato e inserisce remediation automatiche per i casi `assenze` piu' comuni.
- **[feature] Package di conversione**: il parser genera ora un `automation_package.json` con compatibilita', issue, remediation e regole proposte per il portale Django di destinazione.
- **[fix] Supporto export Power Automate reali**: corretto il parsing degli export `.zip` Power Automate con workflow in `properties.definition`, che prima non venivano letti correttamente.
- **[improvement] Estrazione flow piu' utile**: migliorata la normalizzazione di trigger, azioni annidate, rami condizionali, connettori e campi usati, riducendo il rumore del parser iniziale.
- **[docs] README operativo**: aggiunte istruzioni di setup, avvio della UI standalone e descrizione degli output generati.
- **[test] Copertura base della standalone**: aggiunti test su package builder, servizio di conversione e UI Flask.
- **[chore] GitHub hygiene**: aggiornato `.gitignore` per escludere ambiente virtuale, cache Python e artefatti generati in `output/`.
