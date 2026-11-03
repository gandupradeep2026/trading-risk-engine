"""dbt Project Manager, Model Parser & DAG Compiler (Day 23).

Parses dbt project configurations, discovers staging and intermediate models,
analyzes {{ ref(...) }} and {{ source(...) }} dependencies, verifies acyclicity (Kahn's algorithm),
and compiles Jinja-templated GoogleSQL into executable relations.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import re
from typing import Any, Dict, List, Optional, Set, Tuple
import yaml

from dwh.dbt.config import DbtConfig


class DbtProjectError(Exception):
    """Base exception for dbt project parsing and compilation errors."""
    pass


class DbtCycleError(DbtProjectError):
    """Raised when circular dependencies are detected in model refs."""
    pass


@dataclass
class DbtTestSpec:
    """Specification of a declared dbt schema test."""

    test_type: str  # 'unique', 'not_null', 'accepted_values', 'relationships'
    model_name: str
    column_name: str
    values: Optional[List[Any]] = None
    to_model: Optional[str] = None
    to_column: Optional[str] = None


@dataclass
class DbtModelNode:
    """Represents a discovered and parsed dbt model node."""

    name: str
    layer: str  # 'staging', 'intermediate', 'marts'
    file_path: Path
    materialized: str = "view"
    tags: List[str] = field(default_factory=list)
    raw_sql: str = ""
    refs: List[str] = field(default_factory=list)
    sources: List[Tuple[str, str]] = field(default_factory=list)
    description: str = ""
    tests: List[DbtTestSpec] = field(default_factory=list)


class DbtProjectManager:
    """Manages dbt project manifest parsing, dependency resolution, and SQL compilation."""

    def __init__(self, config: Optional[DbtConfig] = None) -> None:
        self.config = config or DbtConfig.from_env()
        self.project_dir = self.config.project_dir
        self.project_manifest: Dict[str, Any] = {}
        self.profiles_manifest: Dict[str, Any] = {}
        self.models: Dict[str, DbtModelNode] = {}
        self.tests: List[DbtTestSpec] = []
        self.sources: Dict[str, List[str]] = {}

        self.load_project_files()
        self.discover_and_parse_models()
        self.load_schema_tests()

    def load_project_files(self) -> None:
        """Load and validate dbt_project.yml and profiles.yml."""
        project_file = self.project_dir / "dbt_project.yml"
        if not project_file.exists():
            raise DbtProjectError(f"dbt_project.yml not found at {project_file}")

        with open(project_file, "r", encoding="utf-8") as f:
            self.project_manifest = yaml.safe_load(f) or {}

        profiles_file = self.project_dir / "profiles.yml"
        if profiles_file.exists():
            with open(profiles_file, "r", encoding="utf-8") as f:
                self.profiles_manifest = yaml.safe_load(f) or {}

    def discover_and_parse_models(self) -> None:
        """Discover all .sql model files across model layer directories."""
        models_root = self.project_dir / "models"
        if not models_root.exists():
            return

        # Check if config.models specifically restricts discovery layers
        allowed_layers: Optional[Set[str]] = None
        if self.config.models and "all" not in self.config.models:
            config_layers = {
                m.lower().replace(".*", "").strip()
                for m in self.config.models
                if m.lower().replace(".*", "").strip() in ("staging", "intermediate", "marts")
            }
            if config_layers:
                allowed_layers = config_layers

        for sql_file in sorted(models_root.glob("**/*.sql")):
            layer = sql_file.parent.name.lower()
            if allowed_layers is not None and layer not in allowed_layers:
                continue
            model_name = sql_file.stem.lower()

            with open(sql_file, "r", encoding="utf-8") as f:
                raw_sql = f.read()

            node = self._parse_model_sql(model_name, layer, sql_file, raw_sql)
            self.models[model_name] = node

    def _parse_model_sql(
        self,
        name: str,
        layer: str,
        file_path: Path,
        raw_sql: str,
    ) -> DbtModelNode:
        """Extract config, refs, and sources from model SQL content."""
        # Parse {{ config(...) }}
        materialized = "table" if layer == "marts" else "view"
        tags: List[str] = [layer]

        config_match = re.search(r"\{\{\s*config\s*\((.*?)\)\s*\}\}", raw_sql, re.DOTALL | re.IGNORECASE)
        if config_match:
            cfg_body = config_match.group(1)
            mat_match = re.search(r"materialized\s*=\s*['\"](\w+)['\"]", cfg_body)
            if mat_match:
                materialized = mat_match.group(1).lower()
            tag_match = re.search(r"tags\s*=\s*\[(.*?)\]", cfg_body)
            if tag_match:
                tags = [t.strip().strip("'\"") for t in tag_match.group(1).split(",") if t.strip()]

        # Parse {{ ref('model_name') }}
        refs: List[str] = []
        for ref_m in re.finditer(r"\{\{\s*ref\s*\(\s*['\"](\w+)['\"]\s*\)\s*\}\}", raw_sql):
            ref_name = ref_m.group(1).lower()
            if ref_name not in refs:
                refs.append(ref_name)

        # Parse {{ source('source_name', 'table_name') }}
        sources: List[Tuple[str, str]] = []
        for src_m in re.finditer(r"\{\{\s*source\s*\(\s*['\"](\w+)['\"]\s*,\s*['\"](\w+)['\"]\s*\)\s*\}\}", raw_sql):
            src_tuple = (src_m.group(1).lower(), src_m.group(2).lower())
            if src_tuple not in sources:
                sources.append(src_tuple)

        return DbtModelNode(
            name=name,
            layer=layer,
            file_path=file_path,
            materialized=materialized,
            tags=tags,
            raw_sql=raw_sql,
            refs=refs,
            sources=sources,
        )

    def load_schema_tests(self) -> None:
        """Parse sources.yml and schema.yml to extract column-level test specifications."""
        models_root = self.project_dir / "models"
        if not models_root.exists():
            return

        for yml_file in models_root.glob("**/*.yml"):
            with open(yml_file, "r", encoding="utf-8") as f:
                content = yaml.safe_load(f) or {}

            # Parse source tests
            if "sources" in content:
                for src in content["sources"]:
                    src_name = src.get("name", "").lower()
                    self.sources[src_name] = [t.get("name", "").lower() for t in src.get("tables", [])]
                    for tbl in src.get("tables", []):
                        tbl_name = tbl.get("name", "").lower()
                        for col in tbl.get("columns", []):
                            col_name = col.get("name", "")
                            for test in col.get("tests", []):
                                self._add_test_spec(tbl_name, col_name, test)

            # Parse model tests
            if "models" in content:
                for mdl in content["models"]:
                    mdl_name = mdl.get("name", "").lower()
                    if mdl_name not in self.models:
                        continue
                    self.models[mdl_name].description = mdl.get("description", "")
                    for col in mdl.get("columns", []):
                        col_name = col.get("name", "")
                        for test in col.get("tests", []):
                            spec = self._add_test_spec(mdl_name, col_name, test)
                            if spec:
                                self.models[mdl_name].tests.append(spec)

    def _add_test_spec(self, model_name: str, column_name: str, test_def: Any) -> Optional[DbtTestSpec]:
        """Convert a test entry in YAML to DbtTestSpec."""
        spec: Optional[DbtTestSpec] = None
        if isinstance(test_def, str):
            spec = DbtTestSpec(test_type=test_def.lower(), model_name=model_name, column_name=column_name)
        elif isinstance(test_def, dict):
            for t_type, t_params in test_def.items():
                t_type_lower = t_type.lower()
                if t_type_lower == "accepted_values":
                    vals = t_params.get("values", []) if isinstance(t_params, dict) else []
                    spec = DbtTestSpec(
                        test_type="accepted_values",
                        model_name=model_name,
                        column_name=column_name,
                        values=vals,
                    )
                elif t_type_lower == "relationships":
                    to_m = t_params.get("to", "") if isinstance(t_params, dict) else ""
                    # Handle ref('parent') in to parameter
                    m = re.search(r"ref\(['\"](\w+)['\"]\)", to_m)
                    parent_model = m.group(1).lower() if m else to_m.lower()
                    to_c = t_params.get("field", "") if isinstance(t_params, dict) else ""
                    spec = DbtTestSpec(
                        test_type="relationships",
                        model_name=model_name,
                        column_name=column_name,
                        to_model=parent_model,
                        to_column=to_c,
                    )
                else:
                    spec = DbtTestSpec(test_type=t_type_lower, model_name=model_name, column_name=column_name)

        if spec:
            self.tests.append(spec)
        return spec

    def get_execution_order(self, selected_models: Optional[List[str]] = None) -> List[str]:
        """Compute the topological execution order of models using Kahn's algorithm."""
        # Determine subset of models
        target_nodes: Set[str] = set()
        if not selected_models or "all" in selected_models:
            target_nodes = set(self.models.keys())
        else:
            for sel in selected_models:
                sel_clean = sel.lower().strip()
                if sel_clean in ("staging", "staging.*"):
                    target_nodes.update(k for k, v in self.models.items() if v.layer == "staging")
                elif sel_clean in ("intermediate", "intermediate.*"):
                    target_nodes.update(k for k, v in self.models.items() if v.layer == "intermediate")
                elif sel_clean in ("marts", "marts.*"):
                    target_nodes.update(k for k, v in self.models.items() if v.layer == "marts")
                elif sel_clean in self.models:
                    target_nodes.add(sel_clean)

        # Build in-degree and adjacency list
        in_degree: Dict[str, int] = {m: 0 for m in target_nodes}
        adj: Dict[str, List[str]] = {m: [] for m in target_nodes}

        for m in target_nodes:
            node = self.models[m]
            for ref in node.refs:
                if ref in target_nodes:
                    adj[ref].append(m)
                    in_degree[m] += 1

        # Kahn's algorithm queue of 0 in-degree nodes
        queue = [m for m, deg in in_degree.items() if deg == 0]
        # Sort queue alphabetically for deterministic ordering
        queue.sort()
        order: List[str] = []

        while queue:
            curr = queue.pop(0)
            order.append(curr)

            for neighbor in sorted(adj[curr]):
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(order) < len(target_nodes):
            remaining = set(target_nodes) - set(order)
            raise DbtCycleError(
                f"Circular dependency detected in dbt models: {sorted(remaining)}"
            )

        return order

    def compile_model_sql(
        self,
        model_name: str,
        target_dataset: Optional[str] = None,
    ) -> str:
        """Compile model SQL by resolving config, ref, source, and surrogate key macros."""
        if model_name not in self.models:
            raise DbtProjectError(f"Model '{model_name}' not found in dbt project catalog.")

        node = self.models[model_name]
        sql = node.raw_sql
        dataset = target_dataset or self.config.dataset_id

        # Strip {{ config(...) }}
        sql = re.sub(r"\{\{\s*config\s*\(.*?\)\s*\}\}", "", sql, flags=re.DOTALL | re.IGNORECASE)

        # Replace {{ ref('target') }} with target relation identifier
        def _replace_ref(match: re.Match[str]) -> str:
            ref_name = match.group(1).lower()
            return f"{dataset}.{ref_name}"

        sql = re.sub(r"\{\{\s*ref\s*\(\s*['\"](\w+)['\"]\s*\)\s*\}\}", _replace_ref, sql)

        # Replace {{ source('src', 'tbl') }} with source table identifier
        def _replace_source(match: re.Match[str]) -> str:
            tbl_name = match.group(2).lower()
            return f"{dataset}.{tbl_name}"

        sql = re.sub(
            r"\{\{\s*source\s*\(\s*['\"](\w+)['\"]\s*,\s*['\"](\w+)['\"]\s*\)\s*\}\}",
            _replace_source,
            sql,
        )

        # Replace {{ var('var_name', 'default') }}
        def _replace_var(match: re.Match[str]) -> str:
            var_name = match.group(1)
            default_val = match.group(2).strip("'\"") if match.group(2) else ""
            val = self.config.vars.get(var_name, getattr(self.config, var_name, default_val))
            return str(val)

        sql = re.sub(
            r"\{\{\s*var\s*\(\s*['\"](\w+)['\"](?:\s*,\s*(['\"].*?['\"]|\w+))?\s*\)\s*\}\}",
            _replace_var,
            sql,
        )

        return sql.strip()
