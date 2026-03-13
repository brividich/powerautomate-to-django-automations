# CHANGELOG

Questo progetto segue un changelog semplice in stile semver, pensato per un repository GitHub: una sezione `Unreleased` sempre in testa e release datate quando si taglia una versione.

## Unreleased

- **[feature] Wizard SQL Server guidato**: la standalone ora parte da una connessione guidata a SQL Server, mostra le tabelle disponibili e permette di scegliere la tabella target prima del caricamento del flow.
- **[feature] Mapping visuale confermabile**: aggiunta la revisione visiva del merge `campo Power Automate -> colonna database`, con conferma o correzione manuale direttamente dalla UI.
- **[feature] Apprendimento locale dei mapping**: le conferme dell'utente vengono salvate in memoria locale e riutilizzate per aumentare la confidenza dei suggerimenti futuri.
- **[improvement] Test indipendenti da file locali**: i test ora usano fixture sintetiche in memoria invece di dipendere da zip non versionati nel workspace.
- **[docs] README aggiornato**: documentati requisiti ODBC, procedura guidata, apprendimento locale e nuovo flusso operativo.

## 0.1.1 - 2026-03-12

- **[release] Prima release stabile della standalone**: pubblicata su `main` la app web locale per analisi, conversione guidata, storico e remediation di export Power Automate.
- **[fix] Robustezza su export anomali**: inclusa la correzione al crash `Analisi fallita: 'str' object has no attribute 'get'` e il parsing difensivo di trigger, azioni e `connectionReferences` non conformi.
- **[test] Verifica consolidata**: confermata la copertura base della standalone, compreso il caso di workflow con shape anomala come `newOP`.

## 0.1.1-dev - 2026-03-12

- **[fix] Parsing difensivo su export anomali**: resa tollerante l'analisi di flow `.zip` / `.json` con blocchi non conformi, ad esempio trigger, azioni o `connectionReferences` serializzati come stringhe invece che come oggetti.
- **[fix] Errore `str.get` in conversione**: eliminato il crash `Analisi fallita: 'str' object has no attribute 'get'` riscontrato con nuovi export Power Automate come `newOP`.
- **[improvement] Diagnostica webapp**: aggiunto logging dell'eccezione lato Flask per facilitare il debug dei casi non coperti dal parser.
- **[test] Copertura su zip malformati**: introdotto un test che valida il comportamento della conversione su workflow con shape non standard.

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
