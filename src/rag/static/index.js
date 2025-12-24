(() => {
    const pageSize = 10;
    let allResults = [];
    let filteredResults = [];
    let currentPage = 1;
    let currentEventSource = null;

    const previewModal = document.getElementById('previewModal');
    const previewFrame = document.getElementById('previewFrame');
    const previewTitle = document.getElementById('previewTitle');
    const aiFloat = document.getElementById('aiFloat');
    const aiToggle = document.getElementById('aiToggle');
    const aiContent = document.getElementById('aiContent');
    const countHint = document.getElementById('countHint');
    const historyList = document.getElementById('historyList');

    function setAIOpen(open) {
        if (open) {
            aiFloat.style.display = 'flex';
            aiFloat.classList.remove('collapsed');
            document.body.classList.add('has-ai-open');
            aiToggle.style.display = 'none';
        } else {
            aiFloat.classList.add('collapsed');
            document.body.classList.remove('has-ai-open');
            aiToggle.style.display = 'inline-flex';
        }
    }

    document.getElementById('aiCollapse').addEventListener('click', () => {
        const isCollapsed = aiFloat.classList.contains('collapsed');
        setAIOpen(isCollapsed);
    });
    aiToggle.addEventListener('click', () => setAIOpen(true));

    // 初始保持收起以居中搜索栏
    setAIOpen(false);

    function closePreview() {
        previewFrame.src = '';
        previewModal.style.display = 'none';
    }

    function openPreview(title, link) {
        previewTitle.textContent = title || '预览';
        previewFrame.src = link;
        previewModal.style.display = 'flex';
    }

    window.closePreview = closePreview;

    function escapeHTML(str = '') {
        return str.replace(/[&<>"']/g, s => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[s]));
    }

    function saveHistory(query) {
        const key = 'ustcSearchHistory';
        const list = JSON.parse(localStorage.getItem(key) || '[]');
        const newList = [query, ...list.filter(q => q !== query)].slice(0, 8);
        localStorage.setItem(key, JSON.stringify(newList));
        renderHistory();
    }

    function renderHistory() {
        const list = JSON.parse(localStorage.getItem('ustcSearchHistory') || '[]');
        if (!list.length) {
            historyList.innerHTML = '<span style="color:#888;font-size:13px;">暂无记录</span>';
            return;
        }
        historyList.innerHTML = '';
        list.forEach(q => {
            const chip = document.createElement('span');
            chip.className = 'history-chip';
            chip.textContent = q;
            chip.onclick = () => {
                document.getElementById('query').value = q;
                search();
            };
            historyList.appendChild(chip);
        });
    }


    function getDocType(doc) {
        const url = doc.fileUrl || doc.url || '';
        const ext = (url.split('.').pop() || '').toLowerCase();
        if (doc.type === 'web') return 'web';
        if (['pdf'].includes(ext)) return 'pdf';
        if (['doc', 'docx'].includes(ext)) return 'word';
        if (['xls', 'xlsx', 'csv'].includes(ext)) return 'excel';
        if (['ppt', 'pptx'].includes(ext)) return 'ppt';
        if (['zip', 'rar', '7z'].includes(ext)) return 'zip';
        if (['html', 'htm'].includes(ext)) return 'web';
        return 'file';
    }

    function buildPreviewLink(doc) {
        const origin = window.location.origin;
        const url = doc.fileUrl || doc.url || '';
        const ext = (url.split('.').pop() || '').toLowerCase();
        const isPage = doc.type === 'web' || ['html', 'htm'].includes(ext);
        if (isPage) return doc.url || url;
        if (!url) return '';
        const base = url.startsWith('http') ? url : `${origin}/file/${url}`;
        
        // 使用微软 Office Online Viewer (需要公网访问)
        if (['doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx'].includes(ext)) {
            return `https://view.officeapps.live.com/op/embed.aspx?src=${encodeURIComponent(base)}`;
        }
        return base;
    }

    function buildDownloadLink(doc) {
        const url = doc.fileUrl || doc.url || '';
        const ext = (url.split('.').pop() || '').toLowerCase();
        const isPage = doc.type === 'web' || ['html', 'htm'].includes(ext);
        if (isPage) return doc.url || url;
        if (!url) return '';
        return url.startsWith('http') ? url : `/download/${url}`;
    }

    function renderPagination() {
        const total = filteredResults.length;
        const totalPages = Math.max(1, Math.ceil(total / pageSize));
        const container = document.getElementById('pagination');
        container.innerHTML = '';
        const createBtn = (label, page, disabled = false, active = false) => {
            const btn = document.createElement('button');
            btn.textContent = label;
            if (active) btn.classList.add('active');
            if (disabled) btn.disabled = true;
            btn.onclick = () => { currentPage = page; renderResults(); };
            return btn;
        };
        container.appendChild(createBtn('上一页', Math.max(1, currentPage - 1), currentPage === 1));
        for (let p = 1; p <= totalPages; p++) {
            if (p === 1 || p === totalPages || Math.abs(p - currentPage) <= 1) {
                container.appendChild(createBtn(p, p, false, p === currentPage));
            } else if (Math.abs(p - currentPage) === 2) {
                const ellipsis = document.createElement('span');
                ellipsis.textContent = '...';
                ellipsis.style.color = '#888';
                container.appendChild(ellipsis);
            }
        }
        container.appendChild(createBtn('下一页', Math.min(totalPages, currentPage + 1), currentPage === totalPages));
    }

    function attachDetailToggle(card) {
        const detailBtn = card.querySelector('.btn-detail');
        const detailPanel = card.querySelector('.detail-panel');
        if (!detailBtn || !detailPanel) return;
        
        // Create close button for detail panel
        const closeBtn = document.createElement('button');
        closeBtn.innerHTML = '&times;';
        closeBtn.className = 'detail-close-btn';
        closeBtn.style.cssText = 'position: absolute; top: 10px; right: 10px; border: none; background: none; font-size: 20px; cursor: pointer; color: #666;';
        detailPanel.appendChild(closeBtn);

        const togglePanel = () => {
            document.querySelectorAll('.detail-panel.show').forEach(p => {
                if (p !== detailPanel) p.classList.remove('show');
            });
            detailPanel.classList.toggle('show');
        };

        detailBtn.addEventListener('click', togglePanel);
        closeBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            detailPanel.classList.remove('show');
        });
    }

    function renderResults() {
        const docList = document.getElementById('docList');
        docList.innerHTML = '';
        if (!filteredResults.length) {
            docList.innerHTML = '<div style="grid-column: 1 / -1; text-align:center; padding:20px;">未找到相关文档</div>';
            document.getElementById('pagination').innerHTML = '';
            return;
        }

        const start = (currentPage - 1) * pageSize;
        const pageItems = filteredResults.slice(start, start + pageSize);
        pageItems.forEach(doc => {
            const title = doc.title && doc.title.trim() ? doc.title : '未命名文件';
            const ext = (doc.fileUrl || doc.url || '').split('.').pop().toLowerCase();
            const typeKey = getDocType(doc);
            let iconClass = 'fa-globe icon-web';
            let typeLabel = '网页';
            if (typeKey === 'pdf') { iconClass = 'fa-file-pdf icon-pdf'; typeLabel = 'PDF'; }
            else if (typeKey === 'word') { iconClass = 'fa-file-word icon-word'; typeLabel = 'Word'; }
            else if (typeKey === 'excel') { iconClass = 'fa-file-excel icon-excel'; typeLabel = 'Excel'; }
            else if (typeKey === 'ppt') { iconClass = 'fa-file-powerpoint icon-ppt'; typeLabel = 'PPT'; }
            else if (typeKey === 'zip') { iconClass = 'fa-file-archive icon-zip'; typeLabel = '压缩包'; }
            else if (typeKey === 'file') { iconClass = 'fa-file-lines'; typeLabel = '文件'; }

            const previewLink = buildPreviewLink(doc);
            const downloadLink = buildDownloadLink(doc);
            const scoreText = (doc.score !== undefined && doc.score !== null) ? Number(doc.score).toFixed(2) : 'N/A';
            const keywords = Array.isArray(doc.keywords) && doc.keywords.length ? doc.keywords : [];
            const keywordTags = keywords.length ? keywords.slice(0, 4).map(k => `<span class="tag">${escapeHTML(k)}</span>`).join('') : '';
            const isPage = doc.type === 'web' || ['html', 'htm'].includes(ext);
            const secondLabel = isPage ? '打开' : '下载';
            const secondIcon = isPage ? 'fa-arrow-up-right-from-square' : 'fa-download';
            const secondTarget = isPage ? '_blank' : '_self';

            const card = document.createElement('div');
            card.className = 'doc-card';
            card.innerHTML = `
                <div class="detail-panel">
                    <div class="detail-content">
                        <h4>📄 内容摘要</h4>
                        <p>${escapeHTML(doc.snippet || '暂无摘要内容...')}</p>
                        <br>
                        <h4>🔗 原始链接</h4>
                        <p class="detail-link">${escapeHTML(doc.url || '')}</p>
                    </div>
                </div>
                <div class="doc-icon-box"><i class="fa ${iconClass}"></i></div>
                <div class="doc-info">
                    <div>
                        <a href="${doc.url || '#'}" target="_blank" class="doc-title" title="${escapeHTML(title)}">${escapeHTML(title)}</a>
                        <div class="doc-meta">
                            <span class="meta-tag type-${typeKey}">${typeLabel}</span>
                            <span class="meta-tag score-tag"><i class="fa fa-fire"></i> ${scoreText}</span>
                        </div>
                        <div class="doc-tags">${keywordTags}</div>
                    </div>
                    <div class="doc-actions">
                        <a href="javascript:void(0)" class="btn-action btn-preview"><i class="fa fa-eye"></i> 预览</a>
                        <a href="javascript:void(0)" class="btn-action btn-detail"><i class="fa fa-circle-info"></i> 详情</a>
                        <a href="${downloadLink || '#'}" target="${secondTarget}" class="btn-action btn-download" ${downloadLink ? '' : 'aria-disabled="true"'}><i class="fa ${secondIcon}"></i> ${secondLabel}</a>
                    </div>
                </div>
            `;
            const previewBtn = card.querySelector('.btn-preview');
            previewBtn.addEventListener('click', () => {
                if (previewLink) openPreview(title, previewLink);
                else window.open(doc.url || '#', '_blank');
            });
            attachDetailToggle(card);
            docList.appendChild(card);
        });

        renderPagination();
    }

    function applyFilters() {
        const rawCheckedTypes = Array.from(document.querySelectorAll('#typeOptions input:checked')).map(i => i.value);
        const checkedTypes = rawCheckedTypes.includes('all') ? [] : rawCheckedTypes.filter(v => v !== 'all');

        filteredResults = allResults.filter(doc => {
            const typeKey = getDocType(doc);
            if (checkedTypes.length && !checkedTypes.includes(typeKey)) return false;

            return true;
        });
        currentPage = 1;
        renderResults();
        countHint.textContent = `已筛选 ${filteredResults.length} 条 / 总计 ${allResults.length} 条`;
    }

    function handleTypeChange(e) {
        const allCheckbox = document.querySelector('#typeOptions input[value="all"]');
        const isAll = e.target.value === 'all';
        if (isAll) {
            if (e.target.checked) {
                document.querySelectorAll('#typeOptions input').forEach(cb => {
                    if (cb.value !== 'all') cb.checked = true;
                });
            }
        } else {
            if (allCheckbox && allCheckbox.checked) allCheckbox.checked = false;
        }
        // 若非“全部”选项全部被勾选，则同步勾选“全部”
        const nonAllChecks = Array.from(document.querySelectorAll('#typeOptions input:not([value="all"])'));
        const anyChecked = nonAllChecks.some(cb => cb.checked);
        const allChecked = nonAllChecks.every(cb => cb.checked);
        if (allCheckbox) {
            if (allChecked) allCheckbox.checked = true;
            if (!anyChecked) allCheckbox.checked = false; // 默认未选任何类型等同于“全部”
        }
        applyFilters();
    }

    function search() {
        const query = document.getElementById('query').value.trim();
        if (!query) return;
        const resultSection = document.getElementById('resultSection');
        const docList = document.getElementById('docList');
        const loading = document.getElementById('loading');
        const searchBtn = document.getElementById('searchBtn');
        resultSection.style.display = 'none';
        aiContent.textContent = '';
        docList.innerHTML = '';
        loading.style.display = 'block';
        searchBtn.disabled = true;

        if (currentEventSource) currentEventSource.close();

        const eventSource = new EventSource(`/api/search?q=${encodeURIComponent(query)}`);
        currentEventSource = eventSource;

        eventSource.addEventListener('results', function(e) {
            loading.style.display = 'none';
            resultSection.style.display = 'block';
            const rawResults = JSON.parse(e.data || '[]');
            const uniqueMap = new Map();
            const normalizeUrl = (url) => {
                if (!url) return '';
                let u = url.trim().toLowerCase();
                u = u.replace(/^https?:\/\//, '');
                if (u.endsWith('/')) u = u.slice(0, -1);
                return u;
            };
            rawResults.forEach(doc => {
                let uniqueKey = (doc.title || '').trim();
                if (!uniqueKey || uniqueKey === '无标题') {
                    if (doc.doc_id) uniqueKey = `id:${doc.doc_id}`;
                    else if (doc.url) uniqueKey = `url:${normalizeUrl(doc.url)}`;
                    else uniqueKey = `content:${(doc.snippet || '').substring(0, 50).trim()}`;
                }
                if (uniqueMap.has(uniqueKey)) {
                    const existing = uniqueMap.get(uniqueKey);
                    const docHasFiles = (doc.file_paths && doc.file_paths.length > 0) || doc.type === 'file';
                    const existingHasFiles = (existing.file_paths && existing.file_paths.length > 0) || existing.type === 'file';
                    if (docHasFiles && !existingHasFiles) {
                        uniqueMap.set(uniqueKey, doc);
                    } else if (docHasFiles === existingHasFiles) {
                        if ((doc.score || 0) > (existing.score || 0)) uniqueMap.set(uniqueKey, doc);
                    }
                } else uniqueMap.set(uniqueKey, doc);
            });
            const results = Array.from(uniqueMap.values()).sort((a, b) => (b.score || 0) - (a.score || 0));
            const refTitle = document.getElementById('refTitle');
            if (refTitle) refTitle.textContent = `📚 相关资料 (共 ${results.length} 条)`;

            allResults = results.map(r => {
                const fileUrl = (r.file_paths && r.file_paths.length) ? r.file_paths[0] : r.url;
                return { ...r, fileUrl };
            });
            applyFilters();
            setAIOpen(true);
            saveHistory(query);
        });

        eventSource.addEventListener('token', function(e) {
            const token = JSON.parse(e.data);
            aiContent.textContent += token;
            setAIOpen(true);
        });

        eventSource.addEventListener('done', function() {
            eventSource.close();
            searchBtn.disabled = false;
        });

        eventSource.onerror = function(e) {
            console.error('EventSource failed:', e);
            eventSource.close();
            loading.style.display = 'none';
            searchBtn.disabled = false;
            if (!aiContent.textContent) {
                aiContent.textContent = '❌ 连接超时或发生错误。';
                setAIOpen(true);
                resultSection.style.display = 'block';
            }
        };
    }

    window.search = search;

    document.querySelectorAll('#typeOptions input').forEach(cb => cb.addEventListener('change', handleTypeChange));
    renderHistory();
})();
