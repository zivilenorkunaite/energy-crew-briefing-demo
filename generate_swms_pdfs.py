#!/usr/bin/env python3
"""Generate SWMS PDFs from swms_content.py."""

import os
import sys

from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
)
from reportlab.lib.enums import TA_LEFT, TA_CENTER

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from server.customise import COLOR_PDF_PRIMARY, COLOR_PDF_ACCENT, COMPANY_NAME
from server.swms_content import SWMS_CONTENT

BRAND_PRIMARY = colors.HexColor(COLOR_PDF_PRIMARY)
BRAND_ACCENT = colors.HexColor(COLOR_PDF_ACCENT)
LIGHT_GREY = colors.HexColor("#f5f5f5")


def make_pdf(doc_name: str, sections: dict, out_path: str):
    doc_ref = doc_name.split(" ", 1)[0]  # "SWMS-001"
    work_type = doc_name.split(" ", 1)[1]  # "Asset Replacement"

    doc = SimpleDocTemplate(
        out_path, pagesize=A4,
        leftMargin=2*cm, rightMargin=2*cm,
        topMargin=2*cm, bottomMargin=2*cm,
    )

    styles = getSampleStyleSheet()
    h2 = ParagraphStyle("H2", parent=styles["Heading2"],
                        textColor=BRAND_PRIMARY, fontSize=11, spaceAfter=3, spaceBefore=8)
    body = ParagraphStyle("Body", parent=styles["Normal"],
                          fontSize=9, leading=13, spaceAfter=3)
    small = ParagraphStyle("Small", parent=styles["Normal"],
                           fontSize=8, leading=11, textColor=colors.grey)

    story = []

    # Header banner
    banner_data = [[
        Paragraph(f"<b>{COMPANY_NAME.upper()}</b>", ParagraphStyle(
            "banner", fontSize=13, textColor=colors.white, leading=16)),
        Paragraph(
            f"<b>SAFE WORK METHOD STATEMENT</b><br/>"
            f"{doc_ref} | Version 1.0 | Effective 1 Jan 2026",
            ParagraphStyle("banner2", fontSize=9, textColor=colors.white, leading=12,
                           alignment=TA_CENTER)
        ),
    ]]
    banner_table = Table(banner_data, colWidths=[8*cm, 9*cm])
    banner_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), BRAND_PRIMARY),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
        ("LEFTPADDING", (0, 0), (0, -1), 10),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    story.append(banner_table)
    story.append(Spacer(1, 0.3*cm))

    # Work type subtitle
    story.append(Paragraph(f"<b>Work Type: {work_type}</b>",
                            ParagraphStyle("wt", fontSize=11, textColor=BRAND_ACCENT, spaceAfter=4)))
    story.append(HRFlowable(width="100%", thickness=2, color=BRAND_ACCENT, spaceAfter=8))

    # Render each section
    for section_title, content in sections.items():
        story.append(Paragraph(section_title, h2))
        for line in content.strip().splitlines():
            line = line.strip()
            if not line:
                story.append(Spacer(1, 0.15*cm))
            elif line.startswith("- "):
                story.append(Paragraph(f"&bull; {line[2:]}", body))
            elif ": " in line and not line.startswith("&"):
                parts = line.split(": ", 1)
                story.append(Paragraph(f"<b>{parts[0]}:</b> {parts[1]}", body))
            else:
                story.append(Paragraph(line, body))

    story.append(Spacer(1, 0.5*cm))
    story.append(HRFlowable(width="100%", thickness=1, color=BRAND_PRIMARY))

    # Sign-off table
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
        ("BACKGROUND", (0, 0), (-1, 0), BRAND_PRIMARY),
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

    # Remove old PDFs
    for f in os.listdir(out_dir):
        if f.endswith(".pdf"):
            os.remove(os.path.join(out_dir, f))

    print(f"Generating {len(SWMS_CONTENT)} SWMS PDFs...")
    for doc_name, sections in SWMS_CONTENT.items():
        doc_ref = doc_name.split(" ", 1)[0]
        work_type = doc_name.split(" ", 1)[1]
        filename = f"{doc_ref}_{work_type.replace(' ', '_')}.pdf"
        path = os.path.join(out_dir, filename)
        make_pdf(doc_name, sections, path)

    print(f"\nGenerated {len(SWMS_CONTENT)} PDFs in {out_dir}/")


if __name__ == "__main__":
    main()
