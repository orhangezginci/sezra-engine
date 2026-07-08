# SEZRA-ENGINE Protokoll

Dieses Dokument ist sprachagnostisch. Es beschreibt, was ein Service
erfüllen muss, um an SEZRA-ENGINE teilzunehmen - unabhängig davon, ob er in
Python, Go, C# oder einer anderen Sprache geschrieben ist. Es gibt keine
gemeinsame Code-Bibliothek, die importiert werden muss. Der einzige Vertrag
ist dieses Dokument plus `envelope.schema.json`.

## 1. Der Event Envelope

Jede Nachricht, die ein Service auf RabbitMQ publiziert, MUSS gegen
`envelope.schema.json` gültig sein. Siehe dort für die genaue Struktur und
`payload-conventions.md` für empfohlene payload-Felder.

Validiere jedes eingehende Envelope gegen das Schema, bevor du es
verarbeitest. Ungültige Envelopes werden verworfen (siehe Abschnitt 4,
Dead-Letter).

## 2. RabbitMQ-Grundregeln

- Alle Exchanges sind vom Typ `fanout` und `durable: true`.
- Namenskonvention für Exchanges: `sezra.stream.<name>`
  (z. B. `sezra.stream.anomaly`, `sezra.stream.analysis`).
- Jeder Service deklariert die Exchanges, die er braucht, selbst
  (`exchange_declare`, idempotent) - kein Service verlässt sich darauf,
  dass ein anderer Service die Exchange bereits angelegt hat.
- Queues sind `durable: true`, Namenskonvention: `sezra.queue.<service-name>`.
- Ein Service bindet seine eigene Queue an die Exchanges, die ihn
  interessieren - er abonniert sich selbst, niemand abonniert für ihn.

## 3. Knowledge-Anreicherung

Knowledge-Builder-Exchanges heißen `sezra.stream.enriched.<was>` (z. B.
`sezra.stream.enriched.semantic`) - benannt nach der Art der Anreicherung,
nicht nach einer Stufennummer (`level1`, `level2`, ...). Grund: mehrere
Knowledge-Builder können unabhängig voneinander verschiedene Dinge zum
selben Envelope beitragen (Master Prompt: "Multiple builders may
contribute to the same event") - eine starre Nummerierung würde eine
lineare Kette suggerieren, die es nicht geben muss.

**Best Practice, keine Pflicht**: Ein nutzerdefinierter/domänenspezifischer
Knowledge-Service sollte in der Regel auf der am weitesten angereicherten
verfügbaren Exchange aufsetzen (z. B. `sezra.stream.enriched.semantic`)
statt direkt auf `sezra.stream.validated` - domänenspezifisches Wissen
profitiert meist von bereits vorhandener generischer Anreicherung. Das ist
aber situativ: manchmal liefert die umgekehrte Reihenfolge (erst
Domänenwissen, z. B. eine ID-Auflösung, dann generische
Semantik-Anreicherung darauf aufbauend) das bessere Ergebnis. Es gibt
keine technische Durchsetzung dieser Reihenfolge - jeder Knowledge-Service
entscheidet selbst, welche Exchange(s) er konsumiert.

## 4. Fehlerbehandlung / Dead-Letter

Jeder Service MUSS zwischen mindestens zwei Fehlerklassen unterscheiden:

- **permanent**: die Nachricht selbst ist fehlerhaft (ungültiges JSON,
  Envelope verletzt das Schema). Wird nie erfolgreich verarbeitbar sein,
  egal wie oft man's versucht.
- **transient**: ein temporäres Problem (z. B. eine abhängige Verbindung
  war kurz nicht erreichbar). Könnte bei einem erneuten Versuch klappen.

Fehlerhafte Nachrichten werden NICHT stillschweigend verworfen, sondern als
eigenes Envelope an `sezra.stream.dead_letter` publiziert, mit
`event_type: "EventProcessingFailed"` und einem Payload, das mindestens
`failed_service`, `failure_class`, `reason` und die Originalnachricht
enthält.

## 5. Was ein Service NICHT tun darf

- Keinen anderen Service direkt aufrufen (kein HTTP, kein direkter Import,
  kein gemeinsames Datenbankschema). Kommunikation ausschließlich über
  RabbitMQ-Events.
- Keine Annahmen über die interne Implementierung anderer Services treffen
  - nur über das, was im Envelope steht.
- Keine domänenspezifische Sonderlogik für einzelne Feldwerte (z. B.
  `if metric_name == "..."`) in generischen Komponenten wie Detektoren
  oder dem Analyzer. Domänenwissen gehört in eine explizit dafür
  vorgesehene Schicht (siehe Master Prompt, "Knowledge Builder Level 3").

## 6. Einstieg für einen neuen Service

1. Lies dieses Dokument und `envelope.schema.json`.
2. Wähle deine Sprache frei.
3. Validiere jedes Envelope, das du empfängst oder publizierst, gegen das
   Schema (jede Sprache hat eine JSON-Schema-Bibliothek).
4. Halte dich an die Namenskonventionen aus Abschnitt 2.
5. Implementiere Dead-Letter-Handling nach Abschnitt 4.

Für Python-Entwickler gibt es zusätzlich ein optionales Scaffold
(`scaffold/python-service/` + `scripts/new-service.sh`), das Schritt 3+4
als Ausgangspunkt vorwegnimmt. Es ist ein Komfort-Startpunkt, keine
Abhängigkeit - der entstehende Service besitzt seine kopierten Dateien
danach vollständig selbst.