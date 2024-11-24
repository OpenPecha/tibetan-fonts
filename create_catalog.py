#!/usr/bin/env python3
import subprocess
import os
import sys
from pathlib import Path

def get_font_metadata(font_file):
    """Extract metadata from a font file using fc-scan."""
    try:
        cmd = ['fc-scan', '--format', '%{family}|%{fullname}|%{postscriptname}\n', str(font_file)]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0 and result.stdout.strip():
            # Process each line of output
            metadata_list = []
            for line in result.stdout.strip().split('\n'):
                family, fullname, postscript = line.split('|')
                # Create set of names (fullname + postscript)
                names = set()
                if fullname.strip():
                    names.update(name.strip() for name in fullname.split(','))
                if postscript.strip():
                    names.add(postscript.strip())
                
                metadata_list.append({
                    'family': family.strip(),
                    'names': names
                })
            return metadata_list
    except Exception as e:
        print(f"Error processing {font_file}: {e}", file=sys.stderr)
    return None

def format_markdown_table(fonts_data):
    """Format the font data as a markdown table."""
    # Table header
    table = "| File | Family | Names |\n"
    table += "|------|--------|-------|\n"
    
    # Table rows
    for font in fonts_data:
        if font['metadata']:
            # Escape pipe characters in all fields
            file_path = font['path'].replace('|', '\\|')
            
            # Create a row for each metadata entry (multiple faces in font file)
            for metadata in font['metadata']:
                family = metadata['family'].replace('|', '\\|')
                names = ', '.join(sorted(metadata['names'])).replace('|', '\\|')
                
                table += f"| {file_path} | {family} | {names} |\n"
        else:
            file_path = font['path'].replace('|', '\\|')
            table += f"| {file_path} | Error | Error |\n"
    
    return table

def main():
    # Get all font files in current directory and subdirectories
    font_extensions = ('.ttf', '.otf', '.ttc')
    fonts_data = []
    
    for font_file in Path('.').rglob('*'):
        if font_file.suffix.lower() in font_extensions:
            # Get relative path
            rel_path = os.path.relpath(font_file)
            metadata = get_font_metadata(font_file)
            fonts_data.append({
                'path': rel_path,
                'metadata': metadata
            })
    
    # Sort by file path for consistent output
    fonts_data.sort(key=lambda x: x['path'])
    
    # Generate and print the markdown table
    print(format_markdown_table(fonts_data))

if __name__ == "__main__":
    main()
