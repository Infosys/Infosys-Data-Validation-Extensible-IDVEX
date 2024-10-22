'''
Copyright 2024 Infosys Ltd.

Use of this source code is governed by MIT license that can be found in the LICENSE file or at

https://opensource.org/licenses/MIT.
'''

import io

from reportlab.lib.colors import HexColor
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import Paragraph, TableStyle, Spacer, LongTable, PageBreak, KeepTogether, Image


def on_page(canvas, doc, pagesize=A4):
    """Function to generate onpage formatting"""
    canvas.setFont('Helvetica', 14)
    page_num = canvas.getPageNumber()
    canvas.setFillColor(HexColor('#333'))
    canvas.drawCentredString(pagesize[0] / 2, 50, str(page_num))


def add_heading(story, title):
    """Function to add header"""
    styles = getSampleStyleSheet()
    custom_para = styles['Heading1']
    custom_para.fontName = 'Helvetica'
    custom_para.fontSize = 14
    custom_para.textColor = HexColor('#8626c3')

    story.append(Spacer(1, 6))
    story.append(Paragraph(f"{title}", custom_para))
    return story


def add_subheading(story, title):
    """Function to add sub heading"""
    styles = getSampleStyleSheet()
    custom_para = styles['Heading2']
    custom_para.fontName = 'Helvetica'
    custom_para.fontSize = 14
    custom_para.textColor = HexColor('#8626c3')

    story.append(Spacer(1, 6))
    story.append(Paragraph(f"{title}", custom_para))
    return story


def add_paragraph(story, title):
    """Function to add paragraph"""
    styles = getSampleStyleSheet()
    custom_para = styles['Normal']
    custom_para.fontName = 'Helvetica'
    custom_para.fontSize = 12
    title_para = Paragraph(f"{title}", custom_para)
    custom_para.textColor = HexColor('#333')

    story.append(Spacer(1, 6))
    story.append(title_para)
    story.append(Spacer(1, 6))
    return story


def add_title(story, title):
    """Function to add title"""
    styles = getSampleStyleSheet()
    custom_para = styles['Title']
    custom_para.fontName = 'Helvetica'
    custom_para.fontSize = 14
    custom_para.textColor = HexColor('#8626c3')

    story.append(Spacer(1, 6))
    story.append(Paragraph(f"<b>{title}</b>", custom_para))
    story.append(Spacer(1, 6))

    return story


def add_table(story, input_data):
    """Function to add table"""
    styles = getSampleStyleSheet()

    table_style = TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), HexColor('#8C2ACC')),
        ('TEXTCOLOR', (0, 0), (-1, 0), HexColor('#FFFFFF')),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica'),
        ('FONTSIZE', (0, 0), (-1, 0), 12),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 10),
        ('BACKGROUND', (0, 1), (-1, -1), HexColor('#FFFFFF')),
        ('TEXTCOLOR', (0, 1), (-1, -1), HexColor('#333')),
        ('ALIGN', (0, 1), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 12),
        ('BOTTOMPADDING', (0, 1), (-1, -1), 6),
        ('GRID', (0, 0), (-1, -1), 1, HexColor('#8626c3')),
    ])

    styles = getSampleStyleSheet()
    custom_para_1 = styles['Normal']
    custom_para_1.fontName = 'Helvetica'
    custom_para_1.fontSize = 12
    custom_para_1.alignment = TA_CENTER
    custom_para_1.textColor = HexColor('#FFFFFF')

    table_data = [[Paragraph(str(col).title(), custom_para_1) for col in input_data.columns]]
    for row in input_data.values:
        table_data.append([Paragraph(str(cell)) for cell in row])

    table = LongTable(table_data,
                      style=table_style,
                      hAlign='CENTER', repeatRows=1)

    story.append(table)

    return story


def add_graph(f, story, title):
    """Function to add graph"""
    styles = getSampleStyleSheet()
    custom_para = styles['Heading3']
    custom_para.fontName = 'Helvetica'
    custom_para.fontSize = 12
    custom_para.textColor = HexColor('#8626c3')

    buf = io.BytesIO()
    f.savefig(buf, bbox_inches='tight', format='png', dpi=400)
    buf.seek(0)
    x, y = f.get_size_inches()

    story.append(KeepTogether(
        [Paragraph(f"{title}", custom_para), Spacer(1, 12), Image(buf, x * inch, y * inch)]))

    story.append(PageBreak())  # Add a page break after each graph

    return story
