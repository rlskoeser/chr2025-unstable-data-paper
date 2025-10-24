#!/usr/bin/env python3

import argparse
import csv
import pathlib

from neuxml import xmlmap
from neuxml.xmlmap import premis as PREMIS


class MetsPremis(xmlmap.XmlObject):
    ROOT_NAMESPACES = {
        "METS": "http://www.loc.gov/METS/",
        "PREMIS": PREMIS.PREMIS_NAMESPACE,
    }
    premis = xmlmap.NodeField(
        "//METS:digiprovMD/METS:mdWrap/METS:xmlData/PREMIS:premis", PREMIS.Premis
    )


def main(mets_dir, output):
    with output.open("w") as outfile:
        fieldnames = ["htid", "event_type", "date", "detail", "filename"]
        csvwriter = csv.DictWriter(outfile, fieldnames=fieldnames)
        csvwriter.writeheader()

        for metsfile in mets_dir.glob("*.mets.xml"):
            mets_xml = xmlmap.load_xmlobject_from_file(metsfile, MetsPremis)
            for event in mets_xml.premis.events:
                csvwriter.writerow(
                    {
                        "htid": mets_xml.premis.object.id,
                        "event_type": event.type,
                        "date": event.date,
                        "detail": event.detail,
                        "filename": metsfile.name
                    }
                )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract events from PREMIS event log in METS xml files"
    )
    parser.add_argument(
        "dir", help="Directory containing mets.xml files", type=pathlib.Path
    )
    parser.add_argument("output", help="Output file", type=pathlib.Path)
    args = parser.parse_args()

    main(args.dir, args.output)
