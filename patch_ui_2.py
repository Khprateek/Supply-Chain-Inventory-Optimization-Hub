import re

path = 'dashboard/templates/index.html'
with open(path, 'r', encoding='utf-8') as f:
    html = f.read()

# Add Arch B Transform compute block
old_ice_trino = '''                        <!-- Down Arrow -->
                        <div class="flex flex-col items-center text-slate-500 h-10 w-full my-2">
                            <div class="data-flow-line h-full w-0.5" id="flow-ice-trino"></div>
                        </div>

                        <!-- Trino Engine -->
                        <div class="card w-full border-slate-700 opacity-80">
                            <h3 class="font-bold text-slate-400 text-sm mb-1 text-center">Trino Engine</h3>
                        </div>'''

new_ice_trino = '''                        <!-- Down Arrow -->
                        <div class="flex flex-col items-center text-purple-500 h-10 w-full my-2 relative">
                            <div class="data-flow-line h-full w-0.5 border-dashed" id="flow-ice-transform"></div>
                            <span class="absolute top-1/2 -translate-y-1/2 bg-slate-900 px-2 text-xs font-mono text-purple-400 border border-purple-500/30 rounded">batch transform</span>
                        </div>
                        
                        <!-- Arch B Transform -->
                        <div class="card w-full border-purple-500/50 shadow-[0_0_15px_rgba(168,85,247,0.1)] mb-2">
                            <div class="flex justify-between items-center border-b border-slate-700 pb-2 mb-3">
                                <h3 class="font-bold text-purple-400">PySpark Batch Compute <span class="ml-2 px-2 py-0.5 bg-purple-500/20 text-purple-300 text-[10px] rounded border border-purple-500/30 uppercase tracking-wide">JSON Parsing</span></h3>
                                <span id="status-transform_iceberg" class="status-dot stopped"></span>
                            </div>
                        </div>

                        <!-- Down Arrow -->
                        <div class="flex flex-col items-center text-purple-500 h-6 w-full my-1">
                            <div class="data-flow-line h-full w-0.5"></div>
                        </div>

                        <!-- Iceberg Mart Storage -->
                        <div class="card w-full border-purple-700/50">
                            <div class="flex items-center">
                                <h3 class="font-bold text-slate-200">Iceberg Structured Mart</h3>
                            </div>
                        </div>'''

html = html.replace(old_ice_trino, new_ice_trino)

# Add control button for transform_iceberg
old_controls = '''                    <div class="flex items-center justify-between pt-2 border-t border-slate-700/50 mt-2">
                        <div><div class="text-sm font-semibold text-blue-300">dbt Transform</div></div>
                        <button id="btn-dbt_run" onclick="toggleProcess('dbt_run')" class="px-3 py-1 text-xs font-bold rounded bg-blue-600 hover:bg-blue-500 text-white w-20 shadow">RUN</button>
                    </div>'''

new_controls = '''                    <div class="flex items-center justify-between pt-2 border-t border-slate-700/50 mt-2">
                        <div><div class="text-sm font-semibold text-blue-300">Arch A Transform</div><div class="text-xs text-slate-500">dbt run</div></div>
                        <button id="btn-dbt_run" onclick="toggleProcess('dbt_run')" class="px-3 py-1 text-xs font-bold rounded bg-blue-600 hover:bg-blue-500 text-white w-20 shadow">RUN</button>
                    </div>
                    <div class="flex items-center justify-between pt-2 border-t border-slate-700/50 mt-2">
                        <div><div class="text-sm font-semibold text-purple-300">Arch B Transform</div><div class="text-xs text-slate-500">PySpark JSON Parse</div></div>
                        <button id="btn-transform_iceberg" onclick="toggleProcess('transform_iceberg')" class="px-3 py-1 text-xs font-bold rounded bg-purple-600 hover:bg-purple-500 text-white w-20 shadow">RUN</button>
                    </div>'''

html = html.replace(old_controls, new_controls)

with open(path, 'w', encoding='utf-8') as f:
    f.write(html)
print("Updated UI HTML")
