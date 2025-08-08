/**
 * 通用数据表选择器组件
 * 用于各个功能模块选择数据库中的数据表
 */

class TableSelector {
    constructor(containerId, options = {}) {
        this.container = document.getElementById(containerId);
        this.options = {
            multiple: false,  // 是否支持多选
            showDetails: true,  // 是否显示表详情
            filterEmpty: true,  // 是否过滤空表
            onSelect: null,  // 选择回调函数
            ...options
        };
        
        this.selectedTables = [];
        this.allTables = [];
        
        this.init();
    }
    
    async init() {
        await this.loadTables();
        this.render();
    }
    
    async loadTables() {
        try {
            const response = await fetch('/api/database_tables');
            const data = await response.json();
            
            if (data.success) {
                this.allTables = data.tables;
                
                // 如果设置了过滤空表，则过滤掉没有数据的表
                if (this.options.filterEmpty) {
                    this.allTables = this.allTables.filter(table => table.has_data);
                }
            } else {
                throw new Error(data.error || '获取数据表失败');
            }
        } catch (error) {
            console.error('加载数据表失败:', error);
            this.showError('加载数据表失败: ' + error.message);
        }
    }
    
    render() {
        if (!this.container) return;
        
        this.container.innerHTML = `
            <div class="table-selector">
                <div class="selector-header">
                    <h3>选择数据表</h3>
                    <div class="selector-stats">
                        共 ${this.allTables.length} 个数据表
                    </div>
                </div>
                
                <div class="selector-search">
                    <input type="text" id="table-search" placeholder="搜索数据表..." class="form-control">
                </div>
                
                <div class="selector-content">
                    <div class="table-list" id="table-list">
                        ${this.renderTableList()}
                    </div>
                </div>
                
                <div class="selector-footer">
                    <div class="selected-info">
                        ${this.options.multiple ? 
                            `已选择 ${this.selectedTables.length} 个表` : 
                            (this.selectedTables.length > 0 ? `已选择: ${this.selectedTables[0].display_name}` : '未选择')
                        }
                    </div>
                </div>
            </div>
        `;
        
        this.bindEvents();
    }
    
    renderTableList() {
        if (this.allTables.length === 0) {
            return '<div class="no-tables">暂无可用的数据表</div>';
        }
        
        return this.allTables.map(table => `
            <div class="table-item ${this.isSelected(table) ? 'selected' : ''}" 
                 data-table-name="${table.name}">
                <div class="table-header">
                    <div class="table-name">
                        <i class="fas fa-table"></i>
                        ${table.display_name}
                        <span class="table-type ${table.type}">${this.getTypeLabel(table.type)}</span>
                    </div>
                    <div class="table-stats">
                        ${table.document_count.toLocaleString()} 条记录 | ${table.field_count} 字段
                    </div>
                </div>
                
                ${this.options.showDetails ? `
                <div class="table-details">
                    <div class="table-info">
                        <div class="info-item">
                            <strong>数据表名:</strong> ${table.name}
                        </div>
                        ${table.source_filename ? `
                        <div class="info-item">
                            <strong>来源文件:</strong> ${table.source_filename}
                        </div>
                        ` : ''}
                        ${table.upload_time ? `
                        <div class="info-item">
                            <strong>导入时间:</strong> ${new Date(table.upload_time).toLocaleString()}
                        </div>
                        ` : ''}
                    </div>
                    
                    <div class="table-fields">
                        <strong>字段预览:</strong>
                        <div class="field-list">
                            ${table.sample_fields.slice(0, 8).map(field => 
                                `<span class="field-tag">${field}</span>`
                            ).join('')}
                            ${table.sample_fields.length > 8 ? '<span class="field-more">...</span>' : ''}
                        </div>
                    </div>
                </div>
                ` : ''}
            </div>
        `).join('');
    }
    
    bindEvents() {
        // 搜索功能
        const searchInput = document.getElementById('table-search');
        if (searchInput) {
            searchInput.addEventListener('input', (e) => {
                this.filterTables(e.target.value);
            });
        }
        
        // 表格选择
        const tableList = document.getElementById('table-list');
        if (tableList) {
            tableList.addEventListener('click', (e) => {
                const tableItem = e.target.closest('.table-item');
                if (tableItem) {
                    const tableName = tableItem.dataset.tableName;
                    this.selectTable(tableName);
                }
            });
        }
    }
    
    filterTables(searchTerm) {
        const filteredTables = this.allTables.filter(table => 
            table.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
            table.display_name.toLowerCase().includes(searchTerm.toLowerCase()) ||
            (table.source_filename && table.source_filename.toLowerCase().includes(searchTerm.toLowerCase()))
        );
        
        const tableList = document.getElementById('table-list');
        if (tableList) {
            tableList.innerHTML = this.renderTableListForTables(filteredTables);
        }
    }
    
    renderTableListForTables(tables) {
        if (tables.length === 0) {
            return '<div class="no-tables">未找到匹配的数据表</div>';
        }
        
        return tables.map(table => `
            <div class="table-item ${this.isSelected(table) ? 'selected' : ''}" 
                 data-table-name="${table.name}">
                <div class="table-header">
                    <div class="table-name">
                        <i class="fas fa-table"></i>
                        ${table.display_name}
                        <span class="table-type ${table.type}">${this.getTypeLabel(table.type)}</span>
                    </div>
                    <div class="table-stats">
                        ${table.document_count.toLocaleString()} 条记录 | ${table.field_count} 字段
                    </div>
                </div>
                
                ${this.options.showDetails ? `
                <div class="table-details">
                    <div class="table-info">
                        <div class="info-item">
                            <strong>数据表名:</strong> ${table.name}
                        </div>
                        ${table.source_filename ? `
                        <div class="info-item">
                            <strong>来源文件:</strong> ${table.source_filename}
                        </div>
                        ` : ''}
                        ${table.upload_time ? `
                        <div class="info-item">
                            <strong>导入时间:</strong> ${new Date(table.upload_time).toLocaleString()}
                        </div>
                        ` : ''}
                    </div>
                    
                    <div class="table-fields">
                        <strong>字段预览:</strong>
                        <div class="field-list">
                            ${table.sample_fields.slice(0, 8).map(field => 
                                `<span class="field-tag">${field}</span>`
                            ).join('')}
                            ${table.sample_fields.length > 8 ? '<span class="field-more">...</span>' : ''}
                        </div>
                    </div>
                </div>
                ` : ''}
            </div>
        `).join('');
    }
    
    selectTable(tableName) {
        const table = this.allTables.find(t => t.name === tableName);
        if (!table) return;
        
        if (this.options.multiple) {
            // 多选模式
            const index = this.selectedTables.findIndex(t => t.name === tableName);
            if (index >= 0) {
                // 取消选择
                this.selectedTables.splice(index, 1);
            } else {
                // 添加选择
                this.selectedTables.push(table);
            }
        } else {
            // 单选模式
            this.selectedTables = [table];
        }
        
        // 更新UI
        this.updateSelection();
        
        // 触发回调
        if (this.options.onSelect) {
            this.options.onSelect(this.selectedTables, table);
        }
    }
    
    updateSelection() {
        // 更新选中状态
        const tableItems = document.querySelectorAll('.table-item');
        tableItems.forEach(item => {
            const tableName = item.dataset.tableName;
            if (this.isSelected({name: tableName})) {
                item.classList.add('selected');
            } else {
                item.classList.remove('selected');
            }
        });
        
        // 更新底部信息
        const selectedInfo = document.querySelector('.selected-info');
        if (selectedInfo) {
            if (this.options.multiple) {
                selectedInfo.textContent = `已选择 ${this.selectedTables.length} 个表`;
            } else {
                selectedInfo.textContent = this.selectedTables.length > 0 ? 
                    `已选择: ${this.selectedTables[0].display_name}` : '未选择';
            }
        }
    }
    
    isSelected(table) {
        return this.selectedTables.some(t => t.name === table.name);
    }
    
    getTypeLabel(type) {
        const labels = {
            'imported_csv': 'CSV导入',
            'user_data': '数据表',
            'system': '系统表'
        };
        return labels[type] || '数据表';
    }
    
    getSelectedTables() {
        return this.selectedTables;
    }
    
    clearSelection() {
        this.selectedTables = [];
        this.updateSelection();
    }
    
    showError(message) {
        if (this.container) {
            this.container.innerHTML = `
                <div class="error-message">
                    <i class="fas fa-exclamation-triangle"></i>
                    ${message}
                </div>
            `;
        }
    }
}

// 导出到全局作用域
window.TableSelector = TableSelector;
