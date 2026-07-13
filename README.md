# SEZRA-ENGINE

**Not an alert. Not a summary. An investigation.**

Klassisches Monitoring (Datadog, Prometheus, Grafana-Alerts & Co.) ist gut
darin, festzustellen, **dass** etwas von der Norm abweicht. Es ist schlecht
darin, zu erklären, **warum**. Wer schon einmal um 3 Uhr nachts von einem
Alert geweckt wurde und danach 45 Minuten Log-Archäologie betreiben musste,
kennt den Bruch zwischen "System hat gemeldet" und "Mensch hat verstanden".

SEZRA-ENGINE schließt genau diese Lücke: Es erkennt statistische Anomalien
in Metrik-Zeitreihen **und** durchsucht parallel dazu unstrukturierte
Kontextquellen (E-Mails, Notizen, beliebige Textdaten) nach semantisch
verwandten Ereignissen - zeitlich plausibel, mit sichtbarem
Konfidenz-Score, und im Zweifel ehrlich "keine überzeugende Erklärung
gefunden" statt einer erfundenen Antwort.

## Was SEZRA von klassischem Monitoring unterscheidet

- **Ursache über Datenquellen hinweg, nicht nur innerhalb einer Metrik.**
  Ein Notendurchschnitt fällt - die Ursache steht in einer Rektor-Mail,
  nicht in einer weiteren Kennzahl. Klassische Monitoring-Tools können
  Metrik-zu-Metrik-Korrelationen finden; SEZRA verbindet Metrik-Anomalien
  mit unstrukturiertem Text, weil die Vektorsuche keinen Unterschied
  zwischen den beiden macht.
- **Ehrliche Kausalitäts-Sprache.** Jedes Ergebnis ist explizit als
  semantische Korrelation gekennzeichnet, nie als bewiesene Kausalität.
  Ein Zeitfilter verhindert, dass eine Ursache nach ihrer Wirkung liegen
  kann. Ein Konfidenz-Schwellwert verhindert, dass schwache Treffer als
  Erklärung verkauft werden.
- **Event-Sourcing statt Zustands-Snapshots.** Jede Beobachtung, jede
  Anreicherung, jede Investigation ist ein unveränderliches Event. Wissen
  wird additiv aufgebaut, nie überschrieben - die komplette Historie
  bleibt nachvollziehbar.
- **Hyper-decoupled.** Jeder Service kommuniziert ausschließlich über
  RabbitMQ-Events. Kein Service kennt die interne Implementierung eines
  anderen. Neue Adapter, Detektoren oder Knowledge-Builder lassen sich
  hinzufügen, ohne Bestehendes anzufassen (siehe
  [`contracts/README.md`](contracts/README.md)).
- **Lokal betreibbar, wo Datenschutz zählt.** Embeddings und
  Textgenerierung laufen standardmäßig vollständig lokal über Ollama -
  keine Daten verlassen die eigene Infrastruktur. Für Entwicklung und
  Tests mit unkritischen Daten lässt sich auf Cloud-Anbieter (OpenAI,
  Gemini) umschalten, wenn Geschwindigkeit oder Qualität wichtiger sind
  als Datenschutz.

## Wie das aussieht

Ein Notendurchschnitt fällt signifikant ab. SEZRA erkennt die Anomalie
automatisch, sucht nach zeitlich passendem Kontext, und liefert:

> **math_test_average** fiel von 80.0 auf 45.0 (drop)
>
> **Wahrscheinlichste Erklärung** (51% Konfidenz, zeitlich plausibel):
> "Der vorgezogene Unterrichtsbeginn könnte zu einer geringeren
> Konzentration der Schüler am Morgen führen, was sich möglicherweise in
> einem Abfall der durchschnittlichen Testergebnisse widerspiegelt."
>
> *Ergebnis basiert auf semantischer Ähnlichkeit, keine bewiesene
> Kausalität.*

Das komplette Szenario lässt sich mit einem Befehl reproduzieren, siehe
Quickstart unten.

## Architektur

```
json-adapter-service ──┐
api-service (POST) ────┴──→ sezra.stream.raw
                                    │
                                    ▼
                          ingestion-service (validiert, adapter-agnostisch)
                                    │
                                    ▼
                          sezra.stream.validated
                                    │
        ┌───────────────────────────┼───────────────────────────┐
        ▼                            ▼                            ▼
persistence-service         knowledge-service          deviation-detector-service
(→ Postgres)          (→ sezra.stream.enriched.semantic)   (→ sezra.stream.anomaly)
        ▲                            │                            │
        │                  vectorizing-service                    │
        │                  (Ollama-Embedding → Qdrant)             │
        │                            ▲                            │
        │                            └──────── analyzer-service ◄─┘
        │                            (Qdrant-Suche + LLM-Erklärung)
        └────────────────────────────┴──→ sezra.stream.investigation
                                             (zurück zu persistence-service)

api-service (GET) ←── liest direkt aus Postgres
```

Jeder Pfeil ist eine RabbitMQ-Exchange (fanout), kein direkter
Service-zu-Service-Aufruf. Details zum Vertrag zwischen den Services:
[`contracts/README.md`](contracts/README.md) und
[`contracts/envelope.schema.json`](contracts/envelope.schema.json).

## Services

| Service | Rolle |
|---|---|
| `json-adapter-service` | Beobachtet ein Verzeichnis, envelope't JSON-Dateien |
| `api-service` | HTTP-Gateway: Daten einreichen (POST) und Investigations abfragen (GET) |
| `ingestion-service` | Validiert eingehende Envelopes, adapter-agnostisch |
| `persistence-service` | Schreibt validierte Events und Investigations nach Postgres |
| `knowledge-service` | Reichert Envelopes um `semantic_text` an (Level-1-Knowledge-Builder) |
| `vectorizing-service` | Erzeugt Embeddings (Ollama) und schreibt sie nach Qdrant |
| `deviation-detector-service` | Erkennt statistische Abweichungen (Z-Score) in Metrik-Reihen |
| `analyzer-service` | Sucht Ursachen-Kandidaten in Qdrant, generiert eine Erklärung per LLM |

Infrastruktur: RabbitMQ, PostgreSQL, Qdrant, Ollama (+ ein-/zweimaliger
Migrations-/Modell-Download-Job für Postgres bzw. Ollama).

## Quickstart

```bash
cp .env.example .env
# .env ausfuellen (Credentials, SEZRA_PROJECT_ID, LLM_PROVIDER)

./scripts/demo-school.sh
```

Das Skript startet den kompletten Stack, reicht ein Beispiel-Szenario
(Notenabfall + Kontext-Mail) per API ein, wartet auf das Ergebnis und
zeigt es an. Danach lässt sich `curl http://localhost:8000/investigations`
jederzeit erneut abfragen.

Für ein lokales, experimentelles Frontend (kein Repo, keine
Abhängigkeiten - einfach eine HTML-Datei, die gegen `api-service`
spricht) siehe die Session-Notizen zu "SEZRA Studio Light".

## Projektstatus

Aktives, persönliches Proof-of-Concept-Projekt. Architektur und Services
sind funktionsfähig und end-to-end getestet, aber die Ausrichtung auf
weitere Domänen-Szenarien (Manufacturing, Healthcare) sowie eine
produktionsreife Absicherung stehen noch aus. Kein Anspruch auf
Vollständigkeit oder Stabilität der API zwischen Versionen.

## Für Entwickler, die einen eigenen Service beitragen wollen

Siehe [`contracts/README.md`](contracts/README.md) - sprachagnostisches
Protokolldokument, das beschreibt, was ein Service erfüllen muss, um an
SEZRA-ENGINE teilzunehmen. Kein Import einer gemeinsamen Bibliothek
nötig, nur der Envelope-Vertrag (`contracts/envelope.schema.json`) und
RabbitMQ.

Für Python-Services existiert ein Scaffold (`scaffold/python-service/` +
`scripts/new-service.sh`), das den Einstieg beschleunigt - komplett
optional, kein Zwang.