"""Generate crew briefing PDFs from markdown response text."""

import io
import re
import aiohttp
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable, Image
)
from reportlab.lib.enums import TA_LEFT, TA_CENTER

from server.customise import COMPANY_NAME, COLOR_PDF_PRIMARY, COLOR_PDF_ACCENT, UC_FULL
from server.config import get_oauth_token, get_workspace_host

BRAND_PRIMARY = colors.HexColor(COLOR_PDF_PRIMARY)
BRAND_ACCENT = colors.HexColor(COLOR_PDF_ACCENT)
LIGHT_GREY = colors.HexColor("#f5f5f5")

# Asset types we can show images for (maps keywords in text → image filename)
ASSET_IMAGE_KEYWORDS = {
    "timber pole": "timber_pole.png",
    "concrete pole": "concrete_pole.png",
    "steel pole": "steel_pole.png",
    "transformer": "pole-mount_transformer.png",
    "pad-mount transformer": "pad-mount_transformer.png",
    "recloser": "recloser.png",
    "switchgear": "circuit_breaker.png",
    "cross-arm": "hardwood_cross-arm.png",
    "conductor": "overhead_conductor.png",
    "underground cable": "underground_cable.png",
    "insulator": "insulator.png",
    "meter": "smart_meter.png",
    "substation": "zone_substation_transformer.png",
    "vegetation": "timber_pole.png",
}


def _fetch_asset_image(filename: str) -> io.BytesIO | None:
    """Fetch an asset image from UC Volume. Returns BytesIO or None."""
    import urllib.request
    try:
        host = get_workspace_host()
        token = get_oauth_token()
        volume_path = f"/Volumes/{UC_FULL.replace('.', '/')}/asset_images/{filename}"
        url = f"{host}/api/2.0/fs/files{volume_path}"
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        resp = urllib.request.urlopen(req, timeout=10)
        return io.BytesIO(resp.read())
    except Exception as e:
        print(f"[PDF] Image fetch failed for {filename}: {e}")
        return None


def _detect_asset_images(text: str) -> list[tuple[str, str]]:
    """Detect asset types mentioned in the briefing text. Returns [(label, filename)]."""
    text_lower = text.lower()
    found = []
    seen = set()
    for keyword, filename in ASSET_IMAGE_KEYWORDS.items():
        if keyword in text_lower and filename not in seen:
            found.append((keyword.title(), filename))
            seen.add(filename)
    return found[:4]  # Max 4 images per briefing


def generate_briefing_pdf(
    response: str,
    title: str = "Crew Briefing",
    crew: str = "",
    briefing_date: str = "",
    sources: list | None = None,
) -> bytes:
    """Generate a PDF from a crew briefing markdown response. Returns PDF bytes."""
    response = _sanitize(response)
    title = _sanitize(title)
    crew = _sanitize(crew)
    briefing_date = _sanitize(briefing_date)
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=2*cm, rightMargin=2*cm,
        topMargin=2*cm, bottomMargin=2*cm,
    )

    styles = getSampleStyleSheet()
    h1 = ParagraphStyle("H1", parent=styles["Heading1"],
                        textColor=BRAND_PRIMARY, fontSize=14, spaceAfter=4)
    h2 = ParagraphStyle("H2", parent=styles["Heading2"],
                        textColor=BRAND_PRIMARY, fontSize=12, spaceAfter=3, spaceBefore=10)
    h3 = ParagraphStyle("H3", parent=styles["Heading3"],
                        textColor=BRAND_PRIMARY, fontSize=10, spaceAfter=2, spaceBefore=6)
    body = ParagraphStyle("Body", parent=styles["Normal"],
                          fontSize=9, leading=13, spaceAfter=3)
    small = ParagraphStyle("Small", parent=styles["Normal"],
                           fontSize=8, leading=11, textColor=colors.grey)
    warning = ParagraphStyle("Warning", parent=styles["Normal"],
                             fontSize=9, leading=13, textColor=colors.HexColor("#c53030"),
                             spaceAfter=3)

    story = []

    # Header banner
    banner_data = [[
        Paragraph(f"<b>{COMPANY_NAME.upper()}</b>", ParagraphStyle(
            "banner", fontSize=13, textColor=colors.white, leading=16)),
        Paragraph(
            f"<b>CREW BRIEFING</b><br/>{briefing_date}" if briefing_date else "<b>CREW BRIEFING</b>",
            ParagraphStyle("banner2", fontSize=10, textColor=colors.white, leading=13,
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

    # Crew subtitle
    if crew:
        story.append(Paragraph(f"<b>{crew}</b>",
                                ParagraphStyle("crew", fontSize=12, textColor=BRAND_ACCENT, spaceAfter=4)))
        story.append(HRFlowable(width="100%", thickness=2, color=BRAND_ACCENT, spaceAfter=8))

    # Parse markdown response into PDF elements
    _parse_markdown(response, story, h2, h3, body, warning)

    # Asset images section
    asset_images = _detect_asset_images(response)
    if asset_images:
        story.append(Spacer(1, 0.3*cm))
        story.append(Paragraph("<b>Related Assets</b>", h2))
        for label, filename in asset_images:
            img_data = _fetch_asset_image(filename)
            if img_data:
                try:
                    story.append(Paragraph(f"<i>{label}</i>", small))
                    story.append(Image(img_data, width=8*cm, height=6*cm, kind='proportional'))
                    story.append(Spacer(1, 0.2*cm))
                except Exception as e:
                    print(f"[PDF] Image embed failed for {filename}: {e}")

    # Sources footer
    if sources:
        story.append(Spacer(1, 0.5*cm))
        story.append(HRFlowable(width="100%", thickness=1, color=BRAND_PRIMARY))
        story.append(Spacer(1, 0.2*cm))
        story.append(Paragraph("<b>Sources</b>", small))
        source_labels = []
        for s in sources:
            stype = s.get("type", "")
            label = s.get("label", "")
            icon = {"genie": "📊", "document": "📋", "weather": "🌤", "web": "🌐"}.get(stype, "•")
            source_labels.append(f"{icon} {label}")
        story.append(Paragraph(" | ".join(source_labels), small))

    # Footer
    story.append(Spacer(1, 0.3*cm))
    story.append(Paragraph(
        "This briefing is AI-generated. Crew Leader must verify all information before departure.",
        small))

    doc.build(story)
    return buf.getvalue()


def _parse_markdown(text: str, story: list, h2, h3, body, warning):
    """Parse markdown text into ReportLab flowables."""
    lines = text.strip().split("\n")
    in_table = False
    table_rows = []
    table_cols = []

    for line in lines:
        stripped = line.strip()

        # Empty line
        if not stripped:
            if in_table and table_rows:
                _flush_table(story, table_rows, table_cols)
                in_table = False
                table_rows = []
                table_cols = []
            story.append(Spacer(1, 0.1*cm))
            continue

        # Table separator
        if re.match(r'^[\|\s\-:]+$', stripped) and "|" in stripped:
            continue

        # Table row
        if stripped.startswith("|") and stripped.endswith("|"):
            cells = [c.strip() for c in stripped.split("|")[1:-1]]
            if not in_table:
                in_table = True
                table_cols = cells
            else:
                table_rows.append(cells)
            continue

        # Flush any pending table
        if in_table and table_rows:
            _flush_table(story, table_rows, table_cols)
            in_table = False
            table_rows = []
            table_cols = []

        # Headings
        if stripped.startswith("## "):
            story.append(Paragraph(_escape(stripped[3:]), h2))
        elif stripped.startswith("### "):
            story.append(Paragraph(_escape(stripped[4:]), h3))
        elif stripped.startswith("# "):
            story.append(Paragraph(_escape(stripped[2:]), h2))
        # Warning lines
        elif stripped.startswith("⚠") or stripped.startswith("**⚠"):
            story.append(Paragraph(f"⚠ {_escape(_strip_bold(stripped))}", warning))
        # Bullet points
        elif stripped.startswith("- ") or stripped.startswith("* "):
            content = _md_to_html(stripped[2:])
            story.append(Paragraph(f"&bull; {content}", body))
        # Bold key-value
        elif "**:" in stripped or stripped.startswith("**"):
            story.append(Paragraph(_md_to_html(stripped), body))
        else:
            story.append(Paragraph(_md_to_html(stripped), body))


def _flush_table(story, rows, cols):
    """Render a markdown table as a ReportLab Table."""
    if not cols:
        return
    all_rows = [[_sanitize(cell) for cell in row] for row in [cols] + rows]
    col_count = len(cols)
    available = 17 * cm
    col_width = available / col_count if col_count else available

    t = Table(all_rows, colWidths=[col_width] * col_count)
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), BRAND_PRIMARY),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.lightgrey),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, LIGHT_GREY]),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(t)
    story.append(Spacer(1, 0.2*cm))


def _sanitize(text: str) -> str:
    """Replace unicode chars that ReportLab's default fonts can't handle."""
    replacements = {
        "\u2026": "...", "\u2013": "-", "\u2014": "--",
        "\u2018": "'", "\u2019": "'", "\u201c": '"', "\u201d": '"',
        "\u2022": "*", "\u2002": " ", "\u2003": " ", "\u00a0": " ",
        "\u2192": "->", "\u2190": "<-", "\u2194": "<->",
        "\u2713": "[v]", "\u2717": "[x]", "\u2605": "*",
        "\u26a0": "!", "\u26a0\ufe0f": "!",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    # Strip any remaining non-latin1 chars
    return text.encode("latin-1", errors="replace").decode("latin-1")


def _escape(text: str) -> str:
    """Escape HTML special chars for ReportLab Paragraph."""
    return _sanitize(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _strip_bold(text: str) -> str:
    """Remove ** markdown bold markers."""
    return text.replace("**", "")


def _md_to_html(text: str) -> str:
    """Convert basic markdown bold/italic to ReportLab HTML."""
    text = _sanitize(text)
    text = _escape(text)
    text = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', text)
    text = re.sub(r'\*(.+?)\*', r'<i>\1</i>', text)
    text = re.sub(r'`(.+?)`', r'<font face="Courier">\1</font>', text)
    return text
