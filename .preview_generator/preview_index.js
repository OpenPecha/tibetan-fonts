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
    
    
    // Font download functionality
    document.addEventListener('click', (ev) => {
        if (ev.target.classList.contains('download-btn')) {
            ev.preventDefault();
            ev.stopPropagation();
            
            const fontPath = ev.target.getAttribute('data-font-path');
            const fontName = ev.target.getAttribute('data-font-name');
            
            if (fontPath && fontName) {
                downloadFont(fontPath, fontName);
            }
        }
    });
    
    function downloadFont(fontPath, fontName) {
        // Create a temporary anchor element for download
        const link = document.createElement('a');
        link.href = fontPath;
        link.download = fontName;
        
        // Set proper MIME type based on file extension
        const extension = fontName.split('.').pop().toLowerCase();
        const mimeTypes = {
            'ttf': 'font/truetype',
            'otf': 'font/opentype', 
            'woff': 'font/woff',
            'woff2': 'font/woff2'
        };
        
        // For direct download, we'll use fetch to ensure proper MIME type
        fetch(fontPath)
            .then(response => {
                if (!response.ok) {
                    throw new Error(`HTTP error! status: ${response.status}`);
                }
                return response.blob();
            })
            .then(blob => {
                // Create a new blob with the correct MIME type
                const mimeType = mimeTypes[extension] || 'application/octet-stream';
                const typedBlob = new Blob([blob], { type: mimeType });
                
                // Create object URL and download
                const url = URL.createObjectURL(typedBlob);
                link.href = url;
                
                // Append to body, click, and remove
                document.body.appendChild(link);
                link.click();
                document.body.removeChild(link);
                
                // Clean up object URL
                URL.revokeObjectURL(url);
            })
            .catch(error => {
                console.error('Download failed:', error);
                alert('Failed to download font file. Please try again.');
            });
    }
    
})()
