"""create projects table

Neue, eigenstaendige Tabelle fuer benannte Analyseprojekte - vorher gab
es project_id nur als loses, mitgefuehrtes Feld auf events ohne eigene
Herkunfts-Quelle (jeder Adapter setzte es hart ueber eine Umgebungs-
variable). Fuer SEZRA Studio's Projekt-Umschalter (Nutzer legt ein
Projekt mit lesbarem Namen an, Engine generiert die UUID) wird jetzt
eine echte Quelle der Wahrheit fuer "welche Projekte gibt es, wie
heissen sie" gebraucht.

Bewusst KEIN Fremdschluessel-Zwang zwischen events.project_id und
projects.id - die Pipeline selbst (Detektoren, knowledge-service etc.)
soll NIE in dieser Tabelle nachschlagen muessen, das wuerde unnoetige
Kopplung einfuehren. Nur api-service kennt projects zusaetzlich, fuer
Verwaltung/Anzeige - lose gekoppelt, wie der Rest der Architektur.

Revision ID: 0003
Revises: 0002
Create Date: 2026-07-26
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "0003"
down_revision: Union[str, None] = "0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "projects",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table("projects")