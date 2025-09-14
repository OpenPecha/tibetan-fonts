

export function getTplParts(src) {
    const tplStart = '@@TPL--'
    const tplEnd = '@@/TPL'

    const tplParts = {
        root: new TplPart('root', src)
    };

    [...src.matchAll(tplStart)].forEach((match) => {
        const startInd = match.index + tplStart.length
        const firstLineEnd = src.indexOf('\n', startInd)
        const endInd = src.indexOf(tplEnd, firstLineEnd)
        const name = src.substring(startInd, firstLineEnd).trim()
        const html = src.substring(firstLineEnd + 1, endInd).trim()
        tplParts[name] = new TplPart(name, html)
    });

    return tplParts
}


class TplPart {
    constructor(name, html) {
        this.name = name
        this._html = html
    }
    
    render(values) {
        let result = this._html
        Object.keys(values).forEach(k => {
            result = result.replaceAll(k, values[k])
        })
        return result
    }
}
