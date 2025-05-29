document.addEventListener('DOMContentLoaded', () => {
    const csvFileInput = document.getElementById('csvFileInput');
    const uploadButton = document.getElementById('uploadButton');
    const messageDiv = document.getElementById('message'); // General upload message
    const uploadedFilesList = document.getElementById('uploadedFilesList');
    const tablesListDiv = document.getElementById('tablesList');
    const noTablesMessage = document.getElementById('noTablesMessage');

    // Modal elements
    const dataPreviewModal = document.getElementById('dataPreviewModal');
    const closeButton = document.querySelector('.close-button');
    const modalTableName = document.getElementById('modalTableName');
    const previewTableHeaderRow = document.getElementById('previewTableHeaderRow');
    const previewTableBody = document.getElementById('previewTableBody');

    // Relationship elements
    const relationshipForm = document.getElementById('relationshipForm');
    const noTablesForRelationshipMessage = document.getElementById('noTablesForRelationshipMessage');
    const sourceTableSelect = document.getElementById('sourceTableSelect');
    const sourceColumnSelect = document.getElementById('sourceColumnSelect');
    const targetTableSelect = document.getElementById('targetTableSelect');
    const targetColumnSelect = document.getElementById('targetColumnSelect');
    const addRelationshipButton = document.getElementById('addRelationshipButton');
    const relationshipMessageDiv = document.getElementById('relationshipMessage'); // Relationship specific message
    const relationshipsListDiv = document.getElementById('relationshipsList');
    const noRelationshipsMessage = document.getElementById('noRelationshipsMessage');

    // Threshold elements
    const thresholdForm = document.getElementById('thresholdForm');
    const noTablesForThresholdMessage = document.getElementById('noTablesForThresholdMessage');
    const thresholdTableSelect = document.getElementById('thresholdTableSelect');
    const thresholdColumnSelect = document.getElementById('thresholdColumnSelect');
    const thresholdFunctionSelect = document.getElementById('thresholdFunctionSelect'); // NEW
    const thresholdOperatorSelect = document.getElementById('thresholdOperatorSelect');
    const thresholdValueInput = document.getElementById('thresholdValueInput');
    const currentValueDisplay = document.getElementById('currentValueDisplay'); // NEW
    const addThresholdButton = document.getElementById('addThresholdButton');
    const thresholdMessageDiv = document.getElementById('thresholdMessage'); // Threshold specific message
    const activeThresholdsListDiv = document.getElementById('activeThresholdsList');
    const noActiveThresholdsMessage = document.getElementById('noActiveThresholdsMessage');

    // Recommendation elements
    const recommendationsListDiv = document.getElementById('recommendationsList');
    const noRecommendationsMessage = document.getElementById('noRecommendationsMessage');


    const BACKEND_URL = 'http://127.0.0.1:5000';
    let allTables = []; // Store all fetched tables for easy access

    // Function to display messages (general purpose)
    function showMessage(text, type, targetElement = messageDiv) {
        targetElement.textContent = text;
        targetElement.className = `message ${type}`;
        setTimeout(() => {
            targetElement.textContent = '';
            targetElement.className = 'message';
        }, 5000);
    }

    // --- Table Fetching and Display ---
    async function fetchAndDisplayTables() {
        try {
            const response = await fetch(`${BACKEND_URL}/tables`);
            const result = await response.json();

            allTables = result.tables || [];
            tablesListDiv.innerHTML = '';
            if (allTables.length > 0) {
                noTablesMessage.style.display = 'none';
                allTables.forEach(table => {
                    const tableItem = document.createElement('div');
                    tableItem.className = 'table-item';
                    tableItem.innerHTML = `
                        <h3>${table.original_filename}</h3> <p>Internal Name: <strong>${table.name}</strong></p> <div class="headers">
                            <strong>Columns:</strong>
                            ${table.headers.map(header => `<span>${header}</span>`).join('')}
                        </div>
                        <div class="actions">
                            <button class="view-data-button" data-table-id="${table.id}">View Data</button>
                        </div>
                    `;
                    tablesListDiv.appendChild(tableItem);
                });
                document.querySelectorAll('.view-data-button').forEach(button => {
                    button.addEventListener('click', (event) => {
                        const tableId = event.target.dataset.tableId;
                        fetchTableData(tableId);
                    });
                });
                populateRelationshipDropdowns(); // Populate relationship dropdowns
                populateThresholdDropdowns();   // Populate threshold dropdowns
            } else {
                noTablesMessage.style.display = 'block';
                relationshipForm.style.display = 'none';
                noTablesForRelationshipMessage.style.display = 'block';
                thresholdForm.style.display = 'none';
                noTablesForThresholdMessage.style.display = 'block';
            }
            fetchAndDisplayRelationships(); // Refresh relationships
            fetchAndDisplayThresholds();    // Refresh thresholds
            fetchAndDisplayRecommendations(); // Refresh recommendations
        } catch (error) {
            console.error('Error fetching tables:', error);
            showMessage("Failed to load tables. Please check backend connection.", "error");
        }
    }

    // --- Table Data Preview ---
    async function fetchTableData(tableId) {
        try {
            modalTableName.textContent = 'Loading data...';
            previewTableHeaderRow.innerHTML = '';
            previewTableBody.innerHTML = '';

            const response = await fetch(`${BACKEND_URL}/tables/${tableId}/data`);
            const result = await response.json();

            if (response.ok) {
                modalTableName.textContent = `Data Preview: ${result.table_name}`;
                result.columns.forEach(col => {
                    const th = document.createElement('th');
                    th.textContent = col; // Note: these are sanitized names from DB
                    previewTableHeaderRow.appendChild(th);
                });

                result.data.forEach(row => {
                    const tr = document.createElement('tr');
                    result.columns.forEach(col => {
                        const td = document.createElement('td');
                        td.textContent = row[col] !== undefined ? row[col] : '';
                        tr.appendChild(td);
                    });
                    previewTableBody.appendChild(tr);
                });

                dataPreviewModal.style.display = 'block';
            } else {
                showMessage(result.error || "Failed to load table data.", "error", messageDiv);
            }
        } catch (error) {
            console.error('Error fetching table data:', error);
            showMessage("Failed to connect to the server or retrieve table data.", "error", messageDiv);
        }
    }

    // --- Relationship Logic (No Changes) ---
    function populateRelationshipDropdowns() {
        sourceTableSelect.innerHTML = '<option value="">Select Source Table</option>';
        targetTableSelect.innerHTML = '<option value="">Select Target Table</option>';
        sourceColumnSelect.innerHTML = '<option value="">Select Source Column</option>';
        targetColumnSelect.innerHTML = '<option value="">Select Target Column</option>';

        if (allTables.length >= 2) {
            noTablesForRelationshipMessage.style.display = 'none';
            relationshipForm.style.display = 'block';
            allTables.forEach(table => {
                const optionSource = document.createElement('option');
                optionSource.value = table.id;
                optionSource.textContent = table.original_filename;
                sourceTableSelect.appendChild(optionSource);

                const optionTarget = document.createElement('option');
                optionTarget.value = table.id;
                optionTarget.textContent = table.original_filename;
                targetTableSelect.appendChild(optionTarget);
            });
        } else {
            noTablesForRelationshipMessage.style.display = 'block';
            relationshipForm.style.display = 'none';
        }
    }

    function updateColumnDropdown(tableId, columnSelectElement) {
        columnSelectElement.innerHTML = '<option value="">Select Column</option>';
        if (tableId) {
            const selectedTable = allTables.find(t => t.id == tableId);
            if (selectedTable && selectedTable.headers) {
                selectedTable.headers.forEach(header => {
                    const option = document.createElement('option');
                    option.value = header;
                    option.textContent = header;
                    columnSelectElement.appendChild(option);
                });
            }
        }
    }

    sourceTableSelect.addEventListener('change', (event) => {
        updateColumnDropdown(event.target.value, sourceColumnSelect);
    });

    targetTableSelect.addEventListener('change', (event) => {
        updateColumnDropdown(event.target.value, targetColumnSelect);
    });

    addRelationshipButton.addEventListener('click', async () => {
        const sourceTableId = sourceTableSelect.value;
        const sourceColumn = sourceColumnSelect.value;
        const targetTableId = targetTableSelect.value;
        const targetColumn = targetColumnSelect.value;

        if (!sourceTableId || !sourceColumn || !targetTableId || !targetColumn) {
            showMessage("Please select all fields for the relationship.", "error", relationshipMessageDiv);
            return;
        }

        if (sourceTableId === targetTableId && sourceColumn === targetColumn) {
            showMessage("Source and Target relationship cannot be identical.", "error", relationshipMessageDiv);
            return;
        }

        try {
            const response = await fetch(`${BACKEND_URL}/relationships`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    source_table_id: parseInt(sourceTableId),
                    source_column: sourceColumn,
                    target_table_id: parseInt(targetTableId),
                    target_column: targetColumn
                })
            });

            const result = await response.json();

            if (response.ok) {
                showMessage(result.message, "success", relationshipMessageDiv);
                fetchAndDisplayRelationships();
                sourceTableSelect.value = "";
                sourceColumnSelect.innerHTML = '<option value="">Select Source Column</option>';
                targetTableSelect.value = "";
                targetColumnSelect.innerHTML = '<option value="">Select Target Column</option>';
            } else {
                showMessage(result.error || "Failed to add relationship.", "error", relationshipMessageDiv);
            }
        } catch (error) {
            console.error('Error adding relationship:', error);
            showMessage("Failed to connect to the server. Could not add relationship.", "error", relationshipMessageDiv);
        }
    });

    async function fetchAndDisplayRelationships() {
        try {
            const response = await fetch(`${BACKEND_URL}/relationships`);
            const result = await response.json();

            relationshipsListDiv.innerHTML = '';
            if (result.relationships && result.relationships.length > 0) {
                noRelationshipsMessage.style.display = 'none';
                result.relationships.forEach(rel => {
                    const relItem = document.createElement('div');
                    relItem.className = 'relationship-item';
                    relItem.innerHTML = `
                        <strong>${rel.source_table_name}</strong>.${rel.source_column}
                        &nbsp;&nbsp;&mdash;&nbsp;&nbsp;
                        <strong>${rel.target_table_name}</strong>.${rel.target_column}
                    `;
                    relationshipsListDiv.appendChild(relItem);
                });
            } else {
                noRelationshipsMessage.style.display = 'block';
            }
        } catch (error) {
            console.error('Error fetching relationships:', error);
            showMessage("Failed to load relationships. Please check backend connection.", "error", relationshipMessageDiv);
        }
    }


    // --- Threshold Logic (MODIFIED) ---

    function populateThresholdDropdowns() {
        thresholdTableSelect.innerHTML = '<option value="">Select Table</option>';
        thresholdColumnSelect.innerHTML = '<option value="">Select Column</option>';
        thresholdFunctionSelect.value = ""; // Reset function dropdown
        thresholdOperatorSelect.value = "";
        thresholdValueInput.value = "";
        currentValueDisplay.textContent = ''; // Clear current value display

        if (allTables.length > 0) {
            noTablesForThresholdMessage.style.display = 'none';
            thresholdForm.style.display = 'block';
            allTables.forEach(table => {
                const option = document.createElement('option');
                option.value = table.id;
                option.textContent = table.original_filename;
                thresholdTableSelect.appendChild(option);
            });
        } else {
            noTablesForThresholdMessage.style.display = 'block';
            thresholdForm.style.display = 'none';
        }
    }

    // Updates column dropdown based on selected table for thresholds
    thresholdTableSelect.addEventListener('change', (event) => {
        updateColumnDropdown(event.target.value, thresholdColumnSelect);
        // Clear function and value when table changes
        thresholdFunctionSelect.value = "";
        thresholdValueInput.value = "";
        currentValueDisplay.textContent = '';
    });

    // Event listener for changes in column or function for live value preview
    thresholdColumnSelect.addEventListener('change', fetchCurrentCalculatedValue);
    thresholdFunctionSelect.addEventListener('change', fetchCurrentCalculatedValue);

    async function fetchCurrentCalculatedValue() {
        const tableId = thresholdTableSelect.value;
        const column_name = thresholdColumnSelect.value;
        const function_name = thresholdFunctionSelect.value;

        currentValueDisplay.textContent = 'Calculating...';
        currentValueDisplay.classList.remove('error-value'); // Remove error style

        if (!tableId || !column_name || !function_name) {
            currentValueDisplay.textContent = ''; // Clear if not enough info selected
            return;
        }

        try {
            const response = await fetch(`${BACKEND_URL}/tables/${tableId}/columns/${column_name}/${function_name}/current_value`);
            const result = await response.json();

            if (response.ok) {
                currentValueDisplay.textContent = `Current ${function_name}(${column_name}) = ${result.current_value}`;
                currentValueDisplay.classList.remove('error-value');
            } else {
                currentValueDisplay.textContent = `Error: ${result.error || 'Failed to get current value.'}`;
                currentValueDisplay.classList.add('error-value'); // Add error style
            }
        } catch (error) {
            console.error('Error fetching current calculated value:', error);
            currentValueDisplay.textContent = 'Error fetching value.';
            currentValueDisplay.classList.add('error-value');
        }
    }


    addThresholdButton.addEventListener('click', async () => {
        const tableId = thresholdTableSelect.value;
        const column_name = thresholdColumnSelect.value;
        const function_name = thresholdFunctionSelect.value; // NEW
        const operator = thresholdOperatorSelect.value;
        const value = thresholdValueInput.value;

        if (!tableId || !column_name || !function_name || !operator || value === null || value === '') {
            showMessage("Please fill all fields for the threshold.", "error", thresholdMessageDiv);
            return;
        }

        try {
            const response = await fetch(`${BACKEND_URL}/thresholds`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    table_id: parseInt(tableId),
                    column_name: column_name,
                    function: function_name, // NEW
                    operator: operator,
                    value: parseFloat(value)
                })
            });

            const result = await response.json();

            if (response.ok) {
                showMessage(result.message, "success", thresholdMessageDiv);
                fetchAndDisplayThresholds();
                // Clear form for next entry
                thresholdTableSelect.value = "";
                thresholdColumnSelect.innerHTML = '<option value="">Select Column</option>';
                thresholdFunctionSelect.value = ""; // NEW
                thresholdOperatorSelect.value = "";
                thresholdValueInput.value = "";
                currentValueDisplay.textContent = ''; // NEW
            } else {
                showMessage(result.error || "Failed to add threshold.", "error", thresholdMessageDiv);
            }
        } catch (error) {
            console.error('Error adding threshold:', error);
            showMessage("Failed to connect to the server. Could not add threshold.", "error", thresholdMessageDiv);
        }
    });

    async function fetchAndDisplayThresholds() {
        try {
            const response = await fetch(`${BACKEND_URL}/thresholds`);
            const result = await response.json();

            activeThresholdsListDiv.innerHTML = '';
            if (result.thresholds && result.thresholds.length > 0) {
                noActiveThresholdsMessage.style.display = 'none';
                result.thresholds.forEach(threshold => {
                    const thresholdItem = document.createElement('div');
                    thresholdItem.className = 'threshold-item';
                    thresholdItem.innerHTML = `
                        <strong>${threshold.function}(${threshold.table_name}.${threshold.column_name})</strong>
                        &nbsp;&nbsp;${threshold.operator}&nbsp;&nbsp;
                        <strong>${threshold.value}</strong>
                    `;
                    activeThresholdsListDiv.appendChild(thresholdItem);
                });
            } else {
                noActiveThresholdsMessage.style.display = 'block';
            }
        } catch (error) {
            console.error('Error fetching thresholds:', error);
            showMessage("Failed to load thresholds. Please check backend connection.", "error", thresholdMessageDiv);
        }
    }


    // --- Recommendation Display Logic (MODIFIED) ---
    async function fetchAndDisplayRecommendations() {
        try {
            const response = await fetch(`${BACKEND_URL}/recommendations`);
            const result = await response.json();

            recommendationsListDiv.innerHTML = '';
            if (result.recommendations && result.recommendations.length > 0) {
                noRecommendationsMessage.style.display = 'none';
                result.recommendations.forEach(rec => {
                    const recItem = document.createElement('div');
                    recItem.className = 'recommendation-item';
                    recItem.innerHTML = `
                        <h4>Recommendation for ${rec.function_name}(${rec.table_name}.${rec.column_name})</h4>
                        <p>${rec.recommendation_text}</p>
                        <div class="details">
                            Triggered when: ${rec.function_name}(${rec.column_name}) ${rec.threshold_operator} ${rec.threshold_value}<br>
                            Current Value: ${rec.current_value}<br>
                            Generated at: ${new Date(rec.timestamp).toLocaleString()}
                        </div>
                    `;
                    recommendationsListDiv.appendChild(recItem);
                });
            } else {
                noRecommendationsMessage.style.display = 'block';
            }
        } catch (error) {
            console.error('Error fetching recommendations:', error);
            showMessage("Failed to load recommendations. Please check backend connection.", "error", messageDiv);
        }
    }


    // --- General Event Listeners and Initial Load ---
    uploadButton.addEventListener('click', async () => {
        const files = csvFileInput.files;
        if (files.length === 0) {
            showMessage("Please select CSV files to upload.", "error");
            return;
        }

        const formData = new FormData();
        for (let i = 0; i < files.length; i++) {
            formData.append('files[]', files[i]);
        }

        uploadedFilesList.innerHTML = '';

        try {
            const response = await fetch(`${BACKEND_URL}/upload`, {
                method: 'POST',
                body: formData
            });

            const result = await response.json();

            if (response.ok) {
                showMessage(result.message, "success");
                fetchAndDisplayTables(); // This now also triggers threshold/recommendation updates
            } else {
                showMessage(result.error || "An unknown error occurred during upload.", "error");
            }
        } catch (error) {
            console.error('Error uploading files:', error);
            showMessage("Failed to connect to the server for upload. Is the backend running?", "error");
        }
    });

    closeButton.addEventListener('click', () => {
        dataPreviewModal.style.display = 'none';
    });

    window.addEventListener('click', (event) => {
        if (event.target == dataPreviewModal) {
            dataPreviewModal.style.display = 'none';
        }
    });

    // Initial fetch of all data when the page loads
    fetchAndDisplayTables();
});