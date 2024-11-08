(() => {

    const docRoot = document.querySelector(':root');
    
    
    window.addEventListener('click', (ev) => {
        const trg = ev.target
        if (trg.classList.contains('acc-title')) {
            trg.parentElement.classList.toggle('__open')
        }
    })
    
    
    const controls = document.querySelector('.controls')

    function updateFz() {
        const val = controls.fz.value
        controls.fz.parentElement.querySelector('b').textContent = val
        docRoot.style.setProperty('--tib-fz', val + 'px')
    }
    
    controls.fz.oninput = updateFz
    updateFz()

    
    const textEls = document.getElementsByClassName('txt')    
    
    function updateText() {
        let val = controls.text.value.trim()
        val = val === '' ? controls.text.dataset.defaultText : val
        for (const textEl of textEls) {
            textEl.textContent = val
        }
    }

    controls.text.oninput = updateText
    updateText()


    
    document.getElementById('toggleExpand').onclick = () => {
        const opened = [...document.querySelectorAll('.acc.__open')]
        
        if (opened.length !== 0) {
            opened.forEach(el => {
                el.classList.remove('__open')
            })
            return
        }

        for (const el of document.querySelectorAll('.acc')) {
            el.classList.add('__open')
        }
    }
    
})()
