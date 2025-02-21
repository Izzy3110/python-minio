import datetime
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas


def generate_pdf(filename):
    # Create a canvas object to generate the PDF
    c = canvas.Canvas(filename, pagesize=letter)

    # Set font and size for text
    c.setFont("Helvetica", 12)

    # Add some text to the PDF
    c.drawString(100, 750, "Hello, this is a sample PDF created with ReportLab!")
    now_date = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S.%f")[:-3]
    c.drawString(100, 730, f"This is another line of text. {now_date}")

    # Save the PDF to file
    c.save()
