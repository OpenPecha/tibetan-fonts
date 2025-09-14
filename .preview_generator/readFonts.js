import fs from 'fs'
import path from 'path'


const FONT_TYPES = {
    'ttf': 'truetype',
    'otf': 'opentype',
    'woff': 'woff',
    'woff2': 'woff2',
    
}
const FONTS_EXT = Object.keys(FONT_TYPES)


export async function readFonts(rootDir, dirToWalk) {
    const fonts = (await Promise.all(dirToWalk.map(async dir => {
        return await readDirectory(rootDir, dir)
    }))).flat()

    const fontsByDir = {}
    
    fonts.forEach(font => {
        const dirRel = font.dirRel
        fontsByDir[dirRel] = fontsByDir[dirRel] || []
        fontsByDir[dirRel].push(font)
    })
    
    const dirs = Object.keys(fontsByDir).sort((a, b) => a.localeCompare(b))
    
    dirs.forEach(dir => {
        fontsByDir[dir].sort((a, b) => a.name.localeCompare(b.name))
    })
    
    return {
        flat: fonts,
        byDir: fontsByDir,
        dirs,
    }
}


async function readDirectory(rootDir, dirIn = '') {
    const dir = path.resolve(dirIn)

    const files = await fs.promises.readdir(dir, {withFileTypes: true, recursive: true})

    return files
        .filter(dirent => FONTS_EXT.includes(getExt(dirent.name)))
        .map(dirent => new Font(rootDir, dirent))
}


function getExt(p) {
    return path.parse(p).ext.slice(1)
}

function idGenCreate() {
    let i = 0
    return () => 'tib-font-' + i++
}

class Font {
    static _idGen = idGenCreate()
    
    constructor(rootDir, dirent) {
        this.id = Font._idGen()
        this._dirent = dirent
        this._rootDir = rootDir
    }
    
    get ext() {
        return getExt(this._dirent.name)
    }
    
    get name() {
        return this._dirent.name
    }
    
    get cssType() {
        return FONT_TYPES[this.ext]
    }
    
    get dirAbs() {
        return this._dirent.path
    }
    
    get dirRel() {
        return path.join(path.relative(this._rootDir, this.dirAbs))
    }
    
    get pathRel() {
        return path.join(this.dirRel, this.name)
    }

    get fontFace() {
        return `
        @font-face {
            font-family: ${this.id};
            src: url("${this.pathRel}") format("${this.cssType}");
        }`
            .replace(/(\s+)/g, ' ')
    }
    
    get cssStyle() {
        return `font-family: ${this.id};`
    }
    
}
