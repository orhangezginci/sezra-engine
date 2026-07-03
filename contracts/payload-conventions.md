# Payload-Konventionen

Der Envelope selbst (`envelope.schema.json`) bleibt bewusst minimal: er enthält
nur Felder, die für **jeden** `event_type` ausnahmslos gelten. Alles Fachliche
lebt im `payload` - dort aber nicht beliebig, sondern nach den folgenden
Konventionen, damit Consumer (z. B. der Analyzer) sich auf konsistente
Feldnamen verlassen können, ohne dass sie im Envelope erzwungen werden müssen.

Diese Konventionen sind **Empfehlungen für Producer**, keine
Schema-Validierung. Ein Event, das eine Konvention nicht erfüllt, ist trotzdem
envelope-gültig - es liefert dem Analyzer/anderen Consumern nur weniger, um
damit zu arbeiten.

---

## `confidence`

**Gilt für:** jeden `event_type`, der eine abgeleitete/unsichere Aussage
trifft (z. B. `AnomalyDetected`, `AnalysisGenerated`,
`InvestigationGenerated`).

**Gilt nicht für:** rohe, unverarbeitete Beobachtungen. Eine Beobachtung ist
einfach ein erfasster Wert - sie hat keine Konfidenz.

```json
"payload": {
  "confidence": 0.82
}
```

- Typ: `float`, Bereich `0.0` - `1.0`
- `1.0` = maximale Sicherheit, `0.0` = keine
- Muss vom erzeugenden Service tatsächlich berechnet sein (z. B. aus einem
  Similarity-Score, einem Z-Score, einer statistischen Konfidenz) -
  niemals ein geschätzter/erfundener Platzhalterwert.

## Zeitliche Quellangaben (kein einheitlicher Feldname)

**Problem, das das löst:** `occurred_at` im Envelope beschreibt nur, wann das
Envelope selbst erzeugt wurde - nicht wann das zugrunde liegende reale
Ereignis stattgefunden hat. Bei verzögerter Ingestion (z. B. eine E-Mail vom
1. Juni wird erst am 5. Juni eingelesen) können beide Zeitpunkte weit
auseinanderliegen. Für korrekte Kausalitäts-/Zeitfilterung braucht der
Analyzer Zugriff auf den realen Zeitpunkt.

**Lösung:** Es gibt bewusst **kein** generisches Envelope-Feld dafür (siehe
Diskussion Brick 1) - stattdessen führt jeder Event-Type sein eigenes,
fachlich benanntes Zeitfeld im payload, passend zur Art der Beobachtung:

| Event-Type-Beispiel | Empfohlenes Payload-Feld |
|---|---|
| E-Mail-/Nachrichten-Kontext | `sent_at` |
| Metrik-Beobachtung | `measured_at` |
| Sonstige externe Beobachtung | `source_timestamp` |

- Typ: ISO 8601, UTC
- Producer, die keinen sinnvollen realen Zeitpunkt kennen (z. B. weil die
  Quelle selbst keinen liefert), lassen das Feld weg - Consumer müssen mit
  Absenz umgehen können (z. B. Fallback auf `occurred_at`, aber mit
  geringerer Kausalitäts-Konfidenz).

## Namenskonflikte mit dem Envelope

`payload` darf keine der sechs reservierten Envelope-Feldnamen
(`schema_version`, `event_id`, `event_type`, `source`, `occurred_at`,
`correlation_id`, `causation_id`) auf oberster Payload-Ebene wiederverwenden,
um Verwechslungen beim Lesen/Debuggen zu vermeiden.