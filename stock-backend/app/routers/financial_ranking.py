from fastapi import APIRouter, Query, HTTPException
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Tuple
from collections import defaultdict
from app.database import SessionLocal
from app.models import FinancialGrowthReport

router = APIRouter(prefix="/financial-ranking", tags=["Financial Ranking"])

# --- Trọng số cho từng chỉ tiêu ---
CRITERIA_WEIGHTS = {
    # nhóm phần trăm
    "loi_nhuan_sau_thue_quy": 1.5,
    "doanh_thu_quy": 1.2,
    "eps_quy": 1.3,
    "loi_nhuan_sau_thue_nam": 2.0,
    "eps_nam": 2.0,
    "dt_nam": 1.0,
    "loi_nhuan_bien_gop_nam": 0.8,
    "loi_nhuan_bien_rong_st_nam": 1.0,
    "roe": 1.5,

    # nhóm tăng trưởng
    "lnst_toc_do_3quy": 1.2,
    "dt_toc_do_3quy": 1.0,
    "eps_toc_do_3quy": 1.0,
    "lnst_toc_do_3nam": 1.5,
    "eps_toc_do_3nam": 1.5,
    "dt_toc_do_3nam": 1.2,
    "su_mo_rong_lnbg": 1.0,
    "su_mo_rong_lnbr_st": 1.0,

    # nhóm liên tiếp
    "lnst_so_quy_lien_tiep_tang_toc": 1.0,
    "dt_so_quy_lien_tiep_tang_toc": 1.0,
    "eps_so_quy_lien_tiep_tang_toc": 1.2,
    "lnst_so_nam_lien_tiep_tang_toc": 1.3,
    "eps_so_nam_lien_tiep_tang_toc": 1.3,
}
# --- Các nhóm tiêu chí theo yêu cầu ---
PERCENT_CRITERIA = [
    "loi_nhuan_sau_thue_quy",
    "doanh_thu_quy",
    "eps_quy",
    "loi_nhuan_sau_thue_nam",
    "eps_nam",
    "dt_nam",
    "loi_nhuan_bien_gop_nam",
    "loi_nhuan_bien_rong_st_nam",
    "roe",
]

GROWTH_CRITERIA = [
    "lnst_toc_do_3quy",
    "dt_toc_do_3quy",
    "eps_toc_do_3quy",
    "lnst_toc_do_3nam",
    "eps_toc_do_3nam",
    "dt_toc_do_3nam",
    "su_mo_rong_lnbg",
    "su_mo_rong_lnbr_st",
]

CONSECUTIVE_CRITERIA = [
    "lnst_so_quy_lien_tiep_tang_toc",
    "dt_so_quy_lien_tiep_tang_toc",
    "eps_so_quy_lien_tiep_tang_toc",
    "lnst_so_nam_lien_tiep_tang_toc",
    "eps_so_nam_lien_tiep_tang_toc",
]


def _sort_and_assign_positions_for_numeric(reports: List[FinancialGrowthReport], field: str) -> List[Tuple[str,int]]:
    """
    For numeric fields (percent and consecutive): sort descending, None -> bottom.
    Return list of (ticker, position) where position starts at 1.
    """
    # produce (ticker, value) list
    pairs = []
    for r in reports:
        v = getattr(r, field, None)
        # None -> -inf to push to bottom
        key = float(v) if v is not None else float("-inf")
        pairs.append((r.ticker, key))

    # stable sort by value desc, then ticker to have deterministic order
    pairs_sorted = sorted(pairs, key=lambda x: (x[1], x[0]), reverse=True)

    # assign position numbers starting 1 (ties keep order but still get distinct positions)
    result = []
    for idx, (t, _) in enumerate(pairs_sorted, start=1):
        result.append((t, idx))
    return result


def _sort_and_assign_positions_for_growth(reports: List[FinancialGrowthReport], field: str) -> List[Tuple[str,int]]:
    """
    For categorical growth fields: prefer 'Tăng tốc' or 'Mở rộng' (group A) >
    'Giảm tốc' or 'Thu hẹp' (group B) > others/None (group C).
    Within a group we order deterministically by ticker name.
    Return list (ticker, position) with position starting at 1.
    """
    group_map = defaultdict(list)  # A, B, C
    for r in reports:
        v = getattr(r, field, None)
        ticker = r.ticker
        if v is None:
            grp = "C"
        else:
            s = str(v)
            if "Tăng tốc" in s or "Mở rộng" in s:
                grp = "A"
            elif "Giảm tốc" in s or "Thu hẹp" in s:
                grp = "B"
            else:
                grp = "C"
        group_map[grp].append(ticker)

    # sort within groups for determinism
    ordered = []
    for grp in ("A", "B", "C"):
        if grp in group_map:
            ordered.extend(sorted(group_map[grp]))

    # assign positions vector[0]=4 VD FPT vector[0]=5 VNM vector[0]=5 score 100 -> vector[0]=0 vector[1]
    # vector dai 1700 FPT: vector = [3,5,0,6,0,0,0,0,0...,0]
    #                 VNM: vector = [3,4,0,2,1,4....] ->score 100
    #                 VCB: vector = [2,6,2,5,4,]->99 [1,2,5,7,5]->98
    result = []
    for idx, t in enumerate(ordered, start=1):
        result.append((t, idx))
    # There might be tickers that are missing from ordered if not present in any groups (shouldn't happen)
    # Ensure every ticker in reports present: append missing at end
    present = set([t for t, _ in result])
    pos = len(result) + 1
    for r in reports:
        if r.ticker not in present:
            result.append((r.ticker, pos))
            pos += 1

    return result


@router.get("/summary")
def ranking_summary(year: int = Query(...), quarter: int = Query(...)):
    """
    Endpoint rút gọn chỉ trả ticker và score.
    """
    db: Session = SessionLocal()
    try:
        # --- tái sử dụng logic từ API chính ---
        reports = db.query(FinancialGrowthReport).filter(
            FinancialGrowthReport.year == year,
            FinancialGrowthReport.quarter == quarter
        ).all()

        if not reports:
            raise HTTPException(status_code=404, detail="No financial_growth_report rows for given year/quarter.")

        tickers = sorted({r.ticker for r in reports})
        counts: Dict[str, Dict[int,int]] = {t: defaultdict(int) for t in tickers}

        def _apply_positions(pos_list: List[Tuple[str,int]], field_name: str):
            w = CRITERIA_WEIGHTS.get(field_name, 1.0)
            for ticker, pos in pos_list:
                counts[ticker][pos] += w
                # FPT [4,....] AAA [5,...]
                # FPT [5,...]  AAA [4.5,....]
        # --- xử lý như API gốc ---
        for field in PERCENT_CRITERIA:
            _apply_positions(_sort_and_assign_positions_for_numeric(reports, field), field)

        for field in GROWTH_CRITERIA:
            _apply_positions(_sort_and_assign_positions_for_growth(reports, field), field)

        for field in CONSECUTIVE_CRITERIA:
            _apply_positions(_sort_and_assign_positions_for_numeric(reports, field), field)

        max_pos = max((max(pos_dict.keys()) if pos_dict else 0) for pos_dict in counts.values())

        def ticker_key_vec(ticker: str):
            d = counts[ticker]
            return tuple(d.get(pos, 0) for pos in range(1, max_pos+1))

        # --- Tính vector thứ hạng ---
        vectors = {t: ticker_key_vec(t) for t in tickers}

                # --- Tính first_nonzero_pos (vị trí tốt nhất mà mã đó có >0) ---
        first_nonzero = {}
        for t, vec in vectors.items():
            fnz = None
            for i, v in enumerate(vec):
                if v and v > 0:
                    fnz = i
                    break
            first_nonzero[t] = fnz  # None nếu toàn zeros

        # --- Nhóm ticker theo first_nonzero_pos ---
        groups_by_pos = defaultdict(list)
        for t, p in first_nonzero.items():
            groups_by_pos[p].append(t)

        # --- Duyệt các pos theo ưu tiên: pos nhỏ (tốt) trước; None xử lý sau ---
        pos_keys = sorted([k for k in groups_by_pos.keys() if k is not None])
        if None in groups_by_pos:
            has_none = True
        else:
            has_none = False

        # --- Gán dense rank theo từng (pos, count) nhóm ---
        ticker_to_rank = {}
        current_rank = 1

        for p in pos_keys:
            tickers_in_group = groups_by_pos[p]
            # Nhóm theo số lượng đạt tại pos p (vector[p])
            count_buckets = defaultdict(list)
            for t in tickers_in_group:
                cnt = vectors[t][p] if p < len(vectors[t]) else 0
                count_buckets[cnt].append(t)

            # Duyệt các count giảm dần (count cao được rank tốt hơn)
            for cnt in sorted(count_buckets.keys(), reverse=True):
                for t in sorted(count_buckets[cnt]):  # deterministic order
                    ticker_to_rank[t] = current_rank
                current_rank += 1

        # Ticker toàn zeros (None) -> xếp sau, cùng rank
        if has_none:
            for t in sorted(groups_by_pos[None]):
                ticker_to_rank[t] = current_rank
            # current_rank += 1  # không cần nếu không dùng sau

        # --- Tạo kết quả rankings kèm rank, score, vector, best_pos, count_at_best_pos ---
        rankings = []
        for t in tickers:
            rank = ticker_to_rank.get(t, current_rank)  # fallback rare case
            # compute score from rank
            score = max(100 - (rank - 1), 1)
            best_pos = first_nonzero[t]
            count_at_best = 0
            if best_pos is not None:
                count_at_best = vectors[t][best_pos] if best_pos < len(vectors[t]) else 0
            rankings.append({
                "ticker": t,
                "score": score,
                "rank": rank,
                "best_pos": best_pos + 1,
                "count_at_best": count_at_best,
                "vector": vectors[t]
            })

        # Sắp xếp: rank tăng dần (rank nhỏ là tốt), nếu cùng rank thì sort ticker
        rankings.sort(key=lambda x: (x["rank"], x["ticker"]))      
        top10_vectors = rankings[:10]

        # print("=== Tổng trọng số của 10 mã cao nhất ===")
        # for r in top10_vectors:
        #     ticker = r["ticker"]
        #     total_weight = sum(vectors[ticker])
        #     print(f"{ticker}: {total_weight:.2f}")
        #     print(top10_vectors)

        return {
            "year": year,
            "quarter": quarter,
            "num_companies": len(tickers), 
            "rankings": [
                {"ticker": r["ticker"], "score": r["score"]}
                for r in rankings
            ]
        }

    finally:
        db.close()
