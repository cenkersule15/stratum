# STRATUM — Plattform für Datenzuverlässigkeit

Lokales Data-Engineering-Projekt von Cenker Sule: von Rohdaten zu geprüften Analysetabellen.

[English](README.md) · [Türkçe](README.tr.md)

## Mit VS Code und Docker starten

ZIP entpacken und den Ordner **stratum** mit `compose.yaml` in VS Code öffnen. Docker Desktop starten, dann im Terminal:

```bash
docker compose up --build
```

**http://localhost:8080** öffnen. Beim ersten Build werden Abhängigkeiten heruntergeladen. Beim ersten API-Start wird die enthaltene echte Stichprobe mit 50.000 Datensätzen verarbeitet. Kein API-Schlüssel nötig. Belegt AFTERLIGHT bereits Port 8080, dessen Container stoppen oder den Host-Port in Compose auf 8081 ändern.

Ergebnisse bleiben im Docker-Volume `stratum-data` erhalten. `docker compose down` löscht sie nicht; `down -v` löscht sie. Für vollständige Monate mindestens 3 GB Docker-Arbeitsspeicher und mehrere GB freien Speicher vorsehen.

## Implementierte Funktionen

- Echte NYC-TLC-Parquet-Daten, Offline-Stichprobe und Download vollständiger Monate Januar-März 2025.
- SHA-256-Prüfung, versionierte Richtlinie und persistentes Ausführungsprotokoll.
- Pflichtspalten, sechs Datensatzregeln, Quarantäne mit Gründen und Vorschau.
- Unveränderte Daten mit identischem Hash und identischer Richtlinie werden nicht erneut publiziert.
- dbt-SQL-Modelle, Tages- und Zonenaggregate sowie zehn Datentests.
- Dagster-Ausführung eines Partitions-Assets und atomische Veröffentlichung nach erfolgreichen Prüfungen.
- Dreisprachige Konsole, Hell-/Dunkelmodus, Datenexplorer, Herkunftsübersicht und JSON-/Log-Export.

## Demonstration

1. Nach dem ersten Lauf: 50.000 Rohdatensätze, 46.797 akzeptiert, 3.203 in Quarantäne.
2. Standardlauf wiederholen: **Unverändert** statt doppelter Publikation.
3. **Fehlende Spalte testen**: `total_amount` fehlt in einer isolierten Kopie; der Vertrag blockiert die Publikation.
4. **Ungültige Entfernung testen**: -999 Meilen in einer Kopie; die Annahmequote fällt unter 80 %, Publikation wird blockiert.
5. Saubere Quelle erneut wählen: Ist sie schon publiziert, wird die Arbeit idempotent übersprungen.
6. Vollständigen Monat herunterladen. Die Monatsdatei ersetzt die Stichprobe derselben Partition, statt sie anzuhängen. Weitere Monate können einzeln nachgeladen werden.

## Datensemantik und Grenzen

Die Januar-Quelldatei enthält 3.475.226 Datensätze. Die enthaltene Stichprobe nimmt jeden 69. Datensatz bis 50.000 Zeilen; sie ist keine zufällige oder repräsentative Stichprobe. Herkunft und Prüfsummen: `data/seed/provenance.json`.

Quarantäne entspricht Projektregeln, nicht zwingend Datenfehlern. Gültige Rückerstattungen oder lange Fahrten können ausgeschlossen werden. Fingerabdrücke identischer Quellzeilen sind Duplikatkandidaten, keine offiziellen Fahrt-IDs. Legitime identische Fahrten können betroffen sein; Rohdateien bleiben erhalten. Beträge sind keine geprüften Umsätze. Zeitangaben bleiben in lokaler Anbieterzeit ohne Zeitzonenumrechnung.

Dagster führt ein Partitions-Asset aus, der Kontrollspeicher protokolliert Unterphasen und dbt beschreibt SQL-Abhängigkeiten. Kein Live-Streaming, CDC, verteilter Scheduler, automatischer Mehrmonatslauf oder Produktions-SLA. Abgebrochene Läufe werden beim Neustart markiert und nicht mitten in einer Phase fortgesetzt. Vollständige technische Details: [English README](README.md).

## Validierung und Lizenz

Native Tests und Frontend-Build siehe `README.md`; Ergebnisse stehen in `docs/VALIDATION.md`. Docker ist in der Autorenumgebung nicht verfügbar, Container-Build und Compose müssen auf dem Zielsystem oder in CI geprüft werden.

Originalcode: MIT. NYC-Daten werden nicht als MIT neu lizenziert; siehe `data/LICENSE.md`. Lokales Einzelbenutzerprojekt ohne Authentifizierung; nicht öffentlich bereitstellen.
