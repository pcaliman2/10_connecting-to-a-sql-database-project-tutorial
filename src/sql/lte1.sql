
CREATE TABLE IF NOT EXISTS lte_IA_info
(
    log_hash bigint,
    posid bigint,
    event_id bigint,
    name text,
    info text,
    lte_earfcn_1 bigint,
    lte_physical_cell_id_1 bigint,
    lte_inst_rsrp_1 real,
    lte_inst_rsrq_1 real,
    lte_inst_rssi_1 real,
    lte_sinr_1 real,
    lte_ta bigint



);

CREATE TABLE IF NOT EXISTS lte_neigh_info
(
    log_hash bigint,
    posid bigint,
    lte_neigh_earfcn_1 bigint,
    lte_neigh_earfcn_2 bigint,
    lte_neigh_earfcn_3 bigint,
    lte_neigh_physical_cell_id_1 bigint,
    lte_neigh_physical_cell_id_2 bigint,
    lte_neigh_physical_cell_id_3 bigint,
    lte_neigh_band_1 bigint,
    lte_neigh_band_2 bigint,
    lte_neigh_band_3 bigint,
    lte_neigh_rsrp_1 real,
    lte_neigh_rsrp_2 real,
    lte_neigh_rsrp_3 real,
    lte_neigh_rsrq_1 real,
    lte_neigh_rsrq_2 real,
    lte_neigh_rsrq_3 real



);





CREATE TABLE IF NOT EXISTS lte_loc
(
    log_hash bigint,
    posid bigint,
    latitude real,
    longitude real,
    speed real


);
