import click
from lxml import etree
from io import DEFAULT_BUFFER_SIZE
from pathlib import Path
from loguru import logger
import sys


@click.command()
@click.option("--input", help="The input RDF/XML file.")
@click.option(
    "--buffer",
    "--buffer-size",
    "buffer_size",
    default=DEFAULT_BUFFER_SIZE,
    help="The file reading buffer size. Influences the size of the bursted files.",
)
@click.option(
    "--output", help="The output directory to write the bursted RDF/XML files to."
)
@click.option("--verbose/--no-verbose", "-v", default=False, help="Enable output.")
def burst(input, output, buffer_size, verbose):
    """
    Read an RDF/XML file as a buffered stream. When ever a buffer is full and a direct child of the root element is closed/ended a new chunk is written to the output directory.
    """
    level = "DEBUG" if verbose else "INFO"
    logger.remove()
    logger.add(sys.stderr, level=level)

    output_dir = Path(output)
    output_dir.mkdir()
    input_file = Path(input)
    burst_count = 0
    with open(input_file, "rb") as file_object:
        parser = etree.XMLPullParser(("start", "end"))
        events = parser.read_events()

        root = None
        while buff := file_object.read(buffer_size):
            logger.debug(".")
            parser.feed(buff)
            descriptions = []
            for action, elem in events:
                if root is None and action == "start":
                    root = elem
                if action == "end" and elem.getparent() == root:
                    logger.debug(f"{action}: {elem.tag}, parent: {elem.getparent()}")
                    descriptions.append(elem)
            if descriptions:
                new_filename = f"{input_file.stem}-{burst_count}{input_file.suffix}"
                with open(output_dir / new_filename, "wb") as output_file:
                    etree.ElementTree(root).write(output_file)
                    logger.debug(etree.tostring(root, encoding="unicode"))
                logger.debug("next file")
                for elem in descriptions:
                    root.remove(elem)
                burst_count += 1
        logger.debug("done")


if __name__ == "__main__":
    burst()
