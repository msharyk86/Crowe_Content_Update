from __future__ import annotations

import io
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

RISK_TAXONOMY_ID_DEFAULT = 80
CONTROL_TAXONOMY_ID_DEFAULT = 66

_DEF_PATTERN = re.compile(r"[^a-z0-9]+")
_ID_PAT = re.compile(r"^(?:FS-)?(.+?-R\d+)(?:-.*)?$", re.IGNORECASE)


@dataclass
class ProcessResult:
    output_bytes: bytes
    output_filename: str
    meta: dict[str, Any]


def norm(value: str) -> str:
    cleaned = str(value).strip().lower()
    cleaned = _DEF_PATTERN.sub("_", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned)
    return cleaned.strip("_")


def detect_file_type(xls: dict[str, pd.DataFrame]) -> str:
    keys_norm = {norm(k): k for k in xls.keys()}
    is_risk = (
        "risk_taxonomy" in keys_norm
        and "risk_category" in keys_norm
        and "risk_definition" in keys_norm
    )
    is_control = (
        "control_taxonomy" in keys_norm
        and "control_category" in keys_norm
        and "control_definition" in keys_norm
    )

    if is_control:
        return "control"
    if is_risk:
        return "risk"
    return "unknown"


def build_sheet_map(xls: dict[str, pd.DataFrame], file_type: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for sheet in xls.keys():
        normalized = norm(sheet)
        if normalized.startswith("instruction"):
            result["Instructions"] = sheet

        if file_type == "risk":
            if normalized.startswith("risk_taxonomy"):
                result["Risk Taxonomy"] = sheet
            elif normalized.startswith("risk_category"):
                result["Risk Category"] = sheet
            elif normalized.startswith("risk_definition"):
                result["Risk Definition"] = sheet
            elif normalized.startswith("business_area_definition") or normalized.startswith(
                "business_area_definitions"
            ):
                result["Business Area Definitions"] = sheet

        if file_type == "control":
            if normalized.startswith("control_taxonomy"):
                result["Control Taxonomy"] = sheet
            elif normalized.startswith("control_category"):
                result["Control Category"] = sheet
            elif normalized.startswith("control_definition"):
                result["Control Definition"] = sheet
            elif normalized.startswith("assessment_template"):
                result["Assessment Templates"] = sheet

    return result


def find_col(df: pd.DataFrame, candidates: list[str]) -> str:
    cols = {norm(c): c for c in df.columns}

    for candidate in candidates:
        candidate_key = norm(candidate)
        if candidate_key in cols:
            return cols[candidate_key]

    for normalized_column, actual_column in cols.items():
        for candidate in candidates:
            if norm(candidate) in normalized_column:
                return actual_column

    raise KeyError(f"Missing any of {candidates} in {list(df.columns)}")


def clean_risk(
    xls: dict[str, pd.DataFrame], sheet_map: dict[str, str], taxonomy_id: int
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, str]]:
    rt_df = xls[sheet_map["Risk Taxonomy"]]
    rc_df = xls[sheet_map["Risk Category"]]
    rd_df = xls[sheet_map["Risk Definition"]]

    rt_id_col = "Id" if "Id" in rt_df.columns else find_col(rt_df, ["Id"])

    rc_tax_id_col = None
    for candidate in ["Risk Taxonomy Id", "Risk Taxonomy ID", "Risk_Taxonomy_Id"]:
        if candidate in rc_df.columns:
            rc_tax_id_col = candidate
            break

    if rc_tax_id_col is None:
        for column in rc_df.columns:
            if norm(column) == "risk_taxonomy_id" or "risk_taxonomy_id" in norm(column):
                rc_tax_id_col = column
                break

    if rc_tax_id_col is None:
        raise KeyError("Risk Category sheet does not contain a Risk Taxonomy Id column.")

    rd_parent_id_col = None
    for candidate in ["Parent Risk Category Id", "Parent Risk Category ID"]:
        if candidate in rd_df.columns:
            rd_parent_id_col = candidate
            break

    if rd_parent_id_col is None:
        for column in rd_df.columns:
            if norm(column).startswith("parent_risk_category_id"):
                rd_parent_id_col = column
                break

    if rd_parent_id_col is None:
        raise KeyError("Risk Definition sheet does not contain a Parent Risk Category Id column.")

    rt = rt_df.copy()
    rt[rt_id_col] = pd.to_numeric(rt[rt_id_col], errors="coerce")
    rt_clean = rt[rt[rt_id_col] == taxonomy_id]

    rc = rc_df.copy()
    rc[rc_tax_id_col] = pd.to_numeric(rc[rc_tax_id_col], errors="coerce")
    rc_clean = rc[rc[rc_tax_id_col] == taxonomy_id]

    kept_category_ids = pd.to_numeric(rc_clean["Id"], errors="coerce").dropna().astype(int).tolist()

    rd = rd_df.copy()
    rd[rd_parent_id_col] = pd.to_numeric(rd[rd_parent_id_col], errors="coerce")
    rd_clean = rd[rd[rd_parent_id_col].isin(kept_category_ids)]

    return (
        rt_clean,
        rc_clean,
        rd_clean,
        {"rc_filter_column": rc_tax_id_col, "rd_parent_col": rd_parent_id_col},
    )


def clean_control(
    xls: dict[str, pd.DataFrame], sheet_map: dict[str, str], taxonomy_id: int
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, str]]:
    ct_df = xls[sheet_map["Control Taxonomy"]]
    cc_df = xls[sheet_map["Control Category"]]
    cd_df = xls[sheet_map["Control Definition"]]

    ct_id_col = "Id" if "Id" in ct_df.columns else find_col(ct_df, ["Id"])

    cc_tax_col = None
    for candidate in ["Control Taxonomy Id", "Control Taxonomy ID", "Control_Taxonomy_Id"]:
        if candidate in cc_df.columns:
            cc_tax_col = candidate
            break

    if cc_tax_col is None:
        for column in cc_df.columns:
            if norm(column) == "control_taxonomy_id" or "control_taxonomy_id" in norm(column):
                cc_tax_col = column
                break

    if cc_tax_col is None:
        raise KeyError("Control Category sheet does not contain a Control Taxonomy Id column.")

    cc_id_col = "Id" if "Id" in cc_df.columns else find_col(cc_df, ["Id"])

    cd_parent_col = None
    for candidate in ["Parent Control Category Id", "Parent Control Category ID"]:
        if candidate in cd_df.columns:
            cd_parent_col = candidate
            break

    if cd_parent_col is None:
        for column in cd_df.columns:
            if norm(column).startswith("parent_control_category_id"):
                cd_parent_col = column
                break

    if cd_parent_col is None:
        raise KeyError(
            "Control Definition sheet does not contain a Parent Control Category Id column."
        )

    ct = ct_df.copy()
    ct[ct_id_col] = pd.to_numeric(ct[ct_id_col], errors="coerce")
    ct_clean = ct[ct[ct_id_col] == taxonomy_id]

    cc = cc_df.copy()
    cc[cc_tax_col] = pd.to_numeric(cc[cc_tax_col], errors="coerce")
    cc_clean = cc[cc[cc_tax_col] == taxonomy_id]

    kept_cc_ids = pd.to_numeric(cc_clean[cc_id_col], errors="coerce").dropna().astype(int).tolist()

    cd = cd_df.copy()
    cd[cd_parent_col] = pd.to_numeric(cd[cd_parent_col], errors="coerce")
    cd_clean = cd[cd[cd_parent_col].isin(kept_cc_ids)]

    return ct_clean, cc_clean, cd_clean, {"cc_filter_column": cc_tax_col, "cd_parent_col": cd_parent_col}


def normalize_id(value: str | None) -> str:
    if value is None:
        return ""

    text = str(value).strip()
    match = _ID_PAT.match(text)
    if match:
        return match.group(1)

    return text[3:] if text.upper().startswith("FS-") else text


def name_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).lower()).strip()


def first_non_blank(series: pd.Series) -> str:
    for value in series:
        string_value = str(value).strip()
        if string_value and string_value.lower() not in {"null", "none", "nan"}:
            return string_value
    return ""


def excel_row_from_index(index: int) -> int:
    return index + 2


def gen_code(name: str, parent_name: str | None = None) -> str:
    token = re.sub(r"[^A-Za-z0-9]+", "", str(name))[:12]
    prefix = ""

    if parent_name:
        parent_initials = "".join(w[0] for w in str(parent_name).split() if w)
        if parent_initials:
            prefix = parent_initials.upper()[:2] + "-"

    return (prefix + token).upper()


def update_risk_stage2_from_details(
    export_book: dict[str, pd.DataFrame],
    sheet_map: dict[str, str],
    details_bytes: bytes,
    risk_taxonomy_id: int = RISK_TAXONOMY_ID_DEFAULT,
) -> dict[str, int]:
    raw = pd.read_excel(io.BytesIO(details_bytes), sheet_name=0, header=None, engine="openpyxl")

    header_idx = None
    for i in range(len(raw)):
        row_vals = [str(v).strip() if pd.notna(v) else "" for v in raw.iloc[i].tolist()]
        if "Risk ID" in row_vals and "Risk Statement" in row_vals:
            header_idx = i
            break

    if header_idx is None:
        for i in range(len(raw)):
            row_vals = [str(v).strip() if pd.notna(v) else "" for v in raw.iloc[i].tolist()]
            if "Risk ID" in row_vals:
                header_idx = i
                break

    if header_idx is None:
        raise RuntimeError("Could not locate header row containing Risk ID in the Risk Details file")

    header = [str(v).strip() if pd.notna(v) else "" for v in raw.iloc[header_idx].tolist()]
    data = raw.iloc[header_idx + 1 :].copy()
    data.columns = header

    rid_col = find_col(data, ["Risk ID"])
    data = data[data[rid_col].notna()].copy()

    stmt_col = None
    cat_col = None
    sub_col = None

    try:
        stmt_col = find_col(data, ["Risk Statement"])
    except KeyError:
        pass

    try:
        cat_col = find_col(data, ["Risk Category"])
    except KeyError:
        pass

    try:
        sub_col = find_col(data, ["Risk Sub-Category", "Risk Sub Category"])
    except KeyError:
        pass

    rid_series = data[rid_col].astype(str).str.strip().apply(normalize_id)
    gb = data.groupby(rid_series)
    details = pd.DataFrame(
        {
            "Risk ID": gb.apply(lambda g: normalize_id(g[rid_col].iloc[0])).values,
            "Risk Statement": gb[stmt_col].apply(first_non_blank) if stmt_col else [""] * gb.ngroups,
            "Risk Category": gb[cat_col].apply(first_non_blank) if cat_col else [""] * gb.ngroups,
            "Risk Sub-Category": gb[sub_col].apply(first_non_blank) if sub_col else [""] * gb.ngroups,
        }
    )

    rc = export_book[sheet_map["Risk Category"]].copy()
    rd = export_book[sheet_map["Risk Definition"]].copy()

    for frame in (rc, rd):
        for candidate in [
            "Parent Risk Category Id",
            "Parent Risk Category Row no",
            "Parent Risk Category Row No",
            "Parent Risk Category Row No*",
            "Parent Risk Category ID",
        ]:
            if candidate in frame.columns and frame[candidate].dtype != "O":
                frame[candidate] = frame[candidate].astype("object")

    rc_id = find_col(rc, ["Id"])
    rc_name = find_col(rc, ["Name*", "Name"])
    rc_desc = find_col(rc, ["Description"])
    rc_tax_id = find_col(rc, ["Risk Taxonomy Id", "Risk Taxonomy Row No"])
    rc_code = find_col(rc, ["Risk Category ID*", "Risk Category ID"])

    rc_parent_id = None
    rc_parent_row = None
    rd_parent_id = None
    rd_parent_row = None

    try:
        rc_parent_id = find_col(rc, ["Parent Risk Category Id", "Parent Risk Category Row no"])
    except KeyError:
        pass

    try:
        rc_parent_row = find_col(rc, ["Parent Risk Category Row no", "Parent Risk Category Row No"])
    except KeyError:
        pass

    rd_id = find_col(rd, ["Id"])
    rd_name = find_col(rd, ["Name*", "Name"])
    rd_desc = find_col(rd, ["Description"])
    rd_status = find_col(rd, ["Status*", "Status"])
    rd_def_code = find_col(rd, ["Risk Definition Id*", "Risk Definition Id"])

    try:
        rd_parent_id = find_col(
            rd,
            ["Parent Risk Category Id", "Parent Risk Category Row No*", "Parent Risk Category Row No"],
        )
    except KeyError:
        pass

    try:
        rd_parent_row = find_col(rd, ["Parent Risk Category Row No*", "Parent Risk Category Row No"])
    except KeyError:
        pass

    namekey_to_row: dict[str, int] = {}
    namekey_to_id: dict[str, int] = {}

    for i, row in rc.iterrows():
        current_name = str(row[rc_name]).strip()
        if not current_name:
            continue

        key = name_key(current_name)
        if key in namekey_to_row:
            continue

        namekey_to_row[key] = i
        numeric_id = pd.to_numeric(row[rc_id], errors="coerce")
        if pd.notna(numeric_id):
            namekey_to_id[key] = int(numeric_id)

    base_to_rd_idx: dict[str, int] = {}
    for idx, value in rd[rd_def_code].items():
        if pd.isna(value):
            continue
        base_to_rd_idx[normalize_id(value)] = idx

    def get_or_create_category(cat_name: str) -> dict[str, Any]:
        normalized_name = str(cat_name).strip() or "UNSPECIFIED"
        key = name_key(normalized_name)

        if key in namekey_to_row:
            row_index = namekey_to_row[key]
            existing_id = namekey_to_id.get(key)
            return {
                "name": normalized_name,
                "row": row_index,
                "excel_row": excel_row_from_index(row_index),
                "id": existing_id,
                "created": False,
            }

        new_row: dict[str, Any] = {
            rc_id: None,
            rc_name: normalized_name,
            rc_desc: normalized_name,
            rc_tax_id: risk_taxonomy_id,
            rc_code: gen_code(normalized_name),
        }

        if rc_parent_id:
            new_row[rc_parent_id] = ""
        if rc_parent_row:
            new_row[rc_parent_row] = ""

        rc.loc[len(rc)] = new_row
        row_index = len(rc) - 1
        namekey_to_row[key] = row_index

        return {
            "name": normalized_name,
            "row": row_index,
            "excel_row": excel_row_from_index(row_index),
            "id": None,
            "created": True,
        }

    def get_or_create_subcategory(sub_name: str, parent_cat: dict[str, Any]) -> dict[str, Any]:
        normalized_name = str(sub_name).strip()
        key = name_key(normalized_name)

        if key in namekey_to_row:
            row_index = namekey_to_row[key]
            existing_id = namekey_to_id.get(key)

            if parent_cat["id"] is not None:
                if rc_parent_id:
                    rc.at[row_index, rc_parent_id] = parent_cat["id"]
                if rc_parent_row:
                    rc.at[row_index, rc_parent_row] = ""
            else:
                if rc_parent_row:
                    rc.at[row_index, rc_parent_row] = parent_cat["excel_row"]
                if rc_parent_id:
                    rc.at[row_index, rc_parent_id] = ""

            return {
                "name": normalized_name,
                "row": row_index,
                "excel_row": excel_row_from_index(row_index),
                "id": existing_id,
                "created": False,
            }

        new_row: dict[str, Any] = {
            rc_id: None,
            rc_name: normalized_name,
            rc_desc: normalized_name,
            rc_tax_id: risk_taxonomy_id,
            rc_code: gen_code(normalized_name, parent_name=parent_cat["name"]),
        }

        if parent_cat["id"] is not None:
            if rc_parent_id:
                new_row[rc_parent_id] = parent_cat["id"]
            if rc_parent_row:
                new_row[rc_parent_row] = ""
        else:
            if rc_parent_row:
                new_row[rc_parent_row] = parent_cat["excel_row"]
            if rc_parent_id:
                new_row[rc_parent_id] = ""

        rc.loc[len(rc)] = new_row
        row_index = len(rc) - 1
        namekey_to_row[key] = row_index

        return {
            "name": normalized_name,
            "row": row_index,
            "excel_row": excel_row_from_index(row_index),
            "id": None,
            "created": True,
        }

    added_defs = 0
    updated_defs = 0

    for _, row in details.iterrows():
        base = row["Risk ID"]
        statement = row.get("Risk Statement") or ""
        category_name = (row.get("Risk Category") or "").strip()
        subcategory_name = (row.get("Risk Sub-Category") or "").strip()

        parent_cat = get_or_create_category(category_name) if category_name else None
        sub_cat = get_or_create_subcategory(subcategory_name, parent_cat) if subcategory_name and parent_cat else None

        existing_idx = base_to_rd_idx.get(base)

        if existing_idx is None:
            new_rd: dict[str, Any] = {
                rd_id: None,
                rd_name: f"{base}-{statement}" if statement else base,
                rd_desc: statement,
                rd_status: "Active",
                rd_def_code: f"FS-{base}",
            }

            if sub_cat:
                if sub_cat["id"] is not None and rd_parent_id:
                    new_rd[rd_parent_id] = sub_cat["id"]
                    if rd_parent_row:
                        new_rd[rd_parent_row] = ""
                elif rd_parent_row:
                    new_rd[rd_parent_row] = sub_cat["excel_row"]
                    if rd_parent_id:
                        new_rd[rd_parent_id] = ""

            rd.loc[len(rd)] = new_rd
            base_to_rd_idx[base] = len(rd) - 1
            added_defs += 1
            continue

        changed = False
        new_name = f"{base}-{statement}" if statement else rd.loc[existing_idx, rd_name]

        if statement and str(rd.loc[existing_idx, rd_desc]) != statement:
            rd.at[existing_idx, rd_desc] = statement
            changed = True

        if statement and str(rd.loc[existing_idx, rd_name]) != new_name:
            rd.at[existing_idx, rd_name] = new_name
            changed = True

        if sub_cat:
            if sub_cat["id"] is not None and rd_parent_id:
                if str(rd.loc[existing_idx, rd_parent_id]) != str(sub_cat["id"]):
                    rd.at[existing_idx, rd_parent_id] = sub_cat["id"]
                    changed = True
                if rd_parent_row:
                    rd.at[existing_idx, rd_parent_row] = ""
            elif rd_parent_row:
                if str(rd.loc[existing_idx, rd_parent_row]) != str(sub_cat["excel_row"]):
                    rd.at[existing_idx, rd_parent_row] = sub_cat["excel_row"]
                    changed = True
                if rd_parent_id:
                    rd.at[existing_idx, rd_parent_id] = ""

        if changed:
            updated_defs += 1

    export_book[sheet_map["Risk Category"]] = rc
    export_book[sheet_map["Risk Definition"]] = rd

    return {
        "details_unique_risks": int(details.shape[0]),
        "added_definitions": int(added_defs),
        "updated_definitions": int(updated_defs),
    }


def _write_workbook(output_book: dict[str, pd.DataFrame], sheet_order: list[str]) -> bytes:
    stream = io.BytesIO()
    with pd.ExcelWriter(stream, engine="openpyxl") as writer:
        for sheet_name in sheet_order:
            frame = output_book[sheet_name]
            frame.to_excel(writer, index=False, sheet_name=sheet_name)
    stream.seek(0)
    return stream.getvalue()


def process_export(
    export_bytes: bytes,
    export_filename: str,
    risk_taxonomy_id: int,
    control_taxonomy_id: int,
    run_stage2: bool,
    risk_details_bytes: bytes | None,
) -> ProcessResult:
    xls = pd.read_excel(io.BytesIO(export_bytes), sheet_name=None, engine="openpyxl")
    original_order = list(xls.keys())
    file_type = detect_file_type(xls)

    if file_type == "unknown":
        raise RuntimeError(f"Unrecognized file type. Sheets found: {list(xls.keys())}")

    sheet_map = build_sheet_map(xls, file_type)
    output_book = dict(xls)
    meta: dict[str, Any] = {"file_type": file_type}

    if file_type == "risk":
        rt_clean, rc_clean, rd_clean, info = clean_risk(xls, sheet_map, risk_taxonomy_id)
        output_book[sheet_map["Risk Taxonomy"]] = rt_clean
        output_book[sheet_map["Risk Category"]] = rc_clean
        output_book[sheet_map["Risk Definition"]] = rd_clean
        meta["stage1"] = {**info, "risk_taxonomy_id": risk_taxonomy_id}

        if run_stage2:
            if not risk_details_bytes:
                raise RuntimeError("Stage 2 selected but no Risk Details file was provided.")
            stage2_info = update_risk_stage2_from_details(
                output_book,
                sheet_map,
                risk_details_bytes,
                risk_taxonomy_id=risk_taxonomy_id,
            )
            meta["stage2"] = stage2_info

    if file_type == "control":
        ct_clean, cc_clean, cd_clean, info = clean_control(xls, sheet_map, control_taxonomy_id)
        output_book[sheet_map["Control Taxonomy"]] = ct_clean
        output_book[sheet_map["Control Category"]] = cc_clean
        output_book[sheet_map["Control Definition"]] = cd_clean
        meta["stage1"] = {**info, "control_taxonomy_id": control_taxonomy_id}

    output_bytes = _write_workbook(output_book, original_order)

    stem = Path(export_filename).stem or "export"
    suffix = "_CLEANED_STAGE2" if file_type == "risk" and run_stage2 else "_CLEANED"
    output_filename = f"{stem}{suffix}.xlsx"

    return ProcessResult(output_bytes=output_bytes, output_filename=output_filename, meta=meta)


app = FastAPI(title="Crowe Content Cleaner API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/clean")
async def clean_file(
    export_file: UploadFile = File(...),
    risk_details_file: UploadFile | None = File(default=None),
    risk_taxonomy_id: int = Form(default=RISK_TAXONOMY_ID_DEFAULT),
    control_taxonomy_id: int = Form(default=CONTROL_TAXONOMY_ID_DEFAULT),
    run_stage2: bool = Form(default=True),
) -> StreamingResponse:
    try:
        export_bytes = await export_file.read()
        details_bytes = await risk_details_file.read() if risk_details_file else None

        result = process_export(
            export_bytes=export_bytes,
            export_filename=export_file.filename or "export.xlsx",
            risk_taxonomy_id=risk_taxonomy_id,
            control_taxonomy_id=control_taxonomy_id,
            run_stage2=run_stage2,
            risk_details_bytes=details_bytes,
        )

    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    headers = {
        "Content-Disposition": f'attachment; filename="{result.output_filename}"',
        "X-Cleaner-Meta": str(result.meta),
    }
    return StreamingResponse(
        io.BytesIO(result.output_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )
