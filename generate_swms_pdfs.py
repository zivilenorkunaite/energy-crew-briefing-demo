#!/usr/bin/env python3
"""Generate SWMS PDFs and upload to Databricks UC Volume."""

import os
import sys

# ── PDF generation ────────────────────────────────────────────────────────────

from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
)
from reportlab.lib.enums import TA_LEFT, TA_CENTER

EE_GREEN = colors.HexColor("#1a4731")
EE_ORANGE = colors.HexColor("#f4a011")
LIGHT_GREY = colors.HexColor("#f5f5f5")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from server.swms import SWMS

DOC_REFS = {
    "Planned Maintenance":    "SWMS-001",
    "Corrective Maintenance": "SWMS-002",
    "Capital Works":          "SWMS-003",
    "Emergency Response":     "SWMS-004",
    "Inspection":             "SWMS-005",
    "Asset Replacement":      "SWMS-006",
    "Vegetation Management":  "SWMS-007",
}


def make_pdf(work_type: str, content: str, out_path: str):
    doc = SimpleDocTemplate(
        out_path, pagesize=A4,
        leftMargin=2*cm, rightMargin=2*cm,
        topMargin=2*cm, bottomMargin=2*cm,
    )

    styles = getSampleStyleSheet()
    h1 = ParagraphStyle("H1", parent=styles["Heading1"],
                        textColor=EE_GREEN, fontSize=14, spaceAfter=4)
    h2 = ParagraphStyle("H2", parent=styles["Heading2"],
                        textColor=EE_GREEN, fontSize=11, spaceAfter=3, spaceBefore=8)
    body = ParagraphStyle("Body", parent=styles["Normal"],
                          fontSize=9, leading=13, spaceAfter=3)
    small = ParagraphStyle("Small", parent=styles["Normal"],
                           fontSize=8, leading=11, textColor=colors.grey)

    story = []

    # Header banner
    banner_data = [[
        Paragraph("<b>ESSENTIAL ENERGY</b>", ParagraphStyle(
            "banner", fontSize=13, textColor=colors.white, leading=16)),
        Paragraph(
            f"<b>SAFE WORK METHOD STATEMENT</b><br/>"
            f"{DOC_REFS[work_type]} | Version 3.x | Effective 1 Jan 2026",
            ParagraphStyle("banner2", fontSize=9, textColor=colors.white, leading=12,
                           alignment=TA_CENTER)
        ),
    ]]
    banner_table = Table(banner_data, colWidths=[8*cm, 9*cm])
    banner_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), EE_GREEN),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
        ("LEFTPADDING", (0, 0), (0, -1), 10),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    story.append(banner_table)
    story.append(Spacer(1, 0.3*cm))

    # Work type subtitle
    story.append(Paragraph(f"<b>Work Type: {work_type}</b>",
                            ParagraphStyle("wt", fontSize=11, textColor=EE_ORANGE, spaceAfter=4)))
    story.append(HRFlowable(width="100%", thickness=2, color=EE_ORANGE, spaceAfter=8))

    # Parse and render content sections
    current_section = None
    for line in content.strip().splitlines():
        line = line.strip()
        if not line:
            story.append(Spacer(1, 0.15*cm))
            continue

        # Section headings (ALL CAPS lines that look like headers)
        if (line.isupper() or (line.endswith(")") and line.split("(")[0].strip().isupper())) \
                and len(line) < 80 and not line.startswith("-"):
            story.append(Paragraph(line, h2))
            current_section = line
        elif line.startswith("- ") or line.startswith("* "):
            story.append(Paragraph(f"• {line[2:]}", body))
        elif ": " in line and not line.startswith("•"):
            # Key: value pair
            parts = line.split(": ", 1)
            story.append(Paragraph(f"<b>{parts[0]}:</b> {parts[1]}", body))
        else:
            story.append(Paragraph(line, body))

    story.append(Spacer(1, 0.5*cm))
    story.append(HRFlowable(width="100%", thickness=1, color=EE_GREEN))

    # Footer sign-off table
    story.append(Spacer(1, 0.3*cm))
    story.append(Paragraph("<b>ACKNOWLEDGEMENT</b>", h2))
    signoff = [
        ["Name", "Role", "Signature", "Date"],
        ["", "Crew Leader", "", ""],
        ["", "Field Supervisor", "", ""],
        ["", "Safety Officer", "", ""],
    ]
    t = Table(signoff, colWidths=[5*cm, 4*cm, 4*cm, 3.5*cm])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), EE_GREEN),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.lightgrey),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, LIGHT_GREY]),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 12),
    ]))
    story.append(t)
    story.append(Spacer(1, 0.3*cm))
    story.append(Paragraph(
        "This SWMS must be read and understood by all workers before commencing work. "
        "Retain signed copy on site for the duration of works.",
        small))

    doc.build(story)
    print(f"  Created: {out_path}")


def main():
    out_dir = os.path.join(os.path.dirname(__file__), "swms_pdfs")
    os.makedirs(out_dir, exist_ok=True)

    pdf_paths = {}
    print("Generating SWMS PDFs...")
    for work_type, content in SWMS.items():
        ref = DOC_REFS[work_type]
        filename = f"{ref}_{work_type.replace(' ', '_')}.pdf"
        path = os.path.join(out_dir, filename)
        make_pdf(work_type, content, path)
        pdf_paths[work_type] = path

    print(f"\nGenerated {len(pdf_paths)} PDFs in {out_dir}/")
    return pdf_paths


if __name__ == "__main__":
    main()
