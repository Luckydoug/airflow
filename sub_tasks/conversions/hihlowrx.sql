Select 
(select 
(case when A.pfrx='2' then 
(case when A.bifocal_progressive='1' then 'High Rx' 
else 
(case when (IFNULL(A.sphre_dv_add,'') not in ('','NONE') or IFNULL(A.sphle_dv_add,'') not in ('','NONE')) then 'High Rx' else 
(case when ((((IFNULL(A.sredvspch,'') in ('+0.25','-0.25','0.00')) and (IFNULL(A.sredvspch,'') in ('-0.25','0.00')))
and ((IFNULL(A.sledvspch,'') in ('+0.25','-0.25','0.00')) and (IFNULL(A.sledvcycl,'') in ('-0.25','0.00'))))
or (((IFNULL(A."U_VSPSRN",'') in ('+0.25','-0.25','0.00')) and (IFNULL(A."U_VSPSRNC",'') in ('-0.25','0.00')))
and ((IFNULL(A.slenv,'') in ('+0.25','-0.25','0.00')) and (IFNULL(A.bothg,'') in ('-0.25','0.00'))))) 
then 'Low Rx' else 'High Rx' end) end) end) 
when A.pfrx='1' then 
(case when (IFNULL(A.progressive,'')='1' or IFNULL(A.bifocal,'')='1') then 'High Rx' else 
(case when (IFNULL(A.old_rxre_dv_ad,'') not in ('','NONE') or IFNULL(A.old_rxle_dv_ad,'') not in ('','NONE')) then 'High Rx' else 
(case when ((((IFNULL(A.old_rxre_dv_sph,'') in ('+0.25','-0.25','0.00')) and (IFNULL(A.old_rxre_dv_cyl,'') in ('-0.25','0.00')))
and ((IFNULL(A.old_rxle_dv_sph,'') in ('+0.25','-0.25','0.00')) and (IFNULL(A.old_rxle_dv_cyl,'') in ('-0.25','0.00'))))
or (((IFNULL(A.old_rxre_nv_sph,'') in ('+0.25','-0.25','0.00')) and (IFNULL(A.old_rxre_nv_cyl,'') in ('-0.25','0.00')))
and ((IFNULL(A.old_rxle_nv_sph,'') in ('+0.25','-0.25','0.00')) and (IFNULL(A.old_rxle_nv_cyl,'') in ('-0.25','0.00')))))
then 'Low Rx' else 'High Rx' end) end) end)
when A.outside_rx='1' then
(case when (IFNULL(A.pgpdv_nvchk,'')='1' or IFNULL(A.progressive,'')='1') then 'High Rx' else 
(case when (IFNULL(A.predv_re_rx_add,'') not in ('','NONE') or IFNULL(A.prenv_le_rx_add_nv,'') not in ('','NONE')) then 'High Rx' else 
(case when ((((IFNULL(A.predvspch,'') in ('+0.25','-0.25','0.00')) and (IFNULL(A.predvcycl,'') in ('-0.25','0.00')))
and ((IFNULL(A.pledvspch,'') in ('+0.25','-0.25','0.00')) and (IFNULL(A.pledvcycl,'') in ('-0.25','0.00'))))
or (((IFNULL(A.prenvspch,'') in ('+0.25','-0.25','0.00')) and (IFNULL(A.prenvcycl,'') in ('-0.25','0.00')))
and ((IFNULL(A.plenvspch,'') in ('+0.25','-0.25','0.00')) and (IFNULL(A.plenvcycl,'') in ('-0.25','0.00')))))
then 'Low Rx' else 'High Rx' end) end) end)
when A.last_rx='1' then
(case when (IFNULL(A."U_VSPLSTPG",'')='1' or IFNULL(A."U_VSPLSTBF",'')='1') then 'High Rx' else 
(case when (IFNULL(A.old_rxre_dv_ad,'') not in ('','NONE') or IFNULL(A.old_rxle_dv_ad,'') not in ('','NONE')) then 'High Rx' else 
(case when ((((IFNULL(A.old_rxre_dv_sph,'') in ('+0.25','-0.25','0.00')) and (IFNULL(A.old_rxre_dv_cyl,'') in ('-0.25','0.00')))
and ((IFNULL(A.old_rxle_dv_sph,'') in ('+0.25','-0.25','0.00')) and (IFNULL(A.old_rxle_dv_cyl,'') in ('-0.25','0.00'))))
or (((IFNULL(A.old_rxre_nv_sph,'') in ('+0.25','-0.25','0.00')) and (IFNULL(A.old_rxre_nv_cyl,'') in ('-0.25','0.00')))
and ((IFNULL(A.old_rxle_nv_sph,'') in ('+0.25','-0.25','0.00')) and (IFNULL(A.old_rxle_nv_cyl,'') in ('-0.25','0.00')))))
then 'Low Rx' else 'High Rx' end) end) end) end) 
from mabawa_dw.fact_prescriptions A where A.code=T1."U_VSPVSID") as "Rx"

from "@VSP_OPT_ORPCT" T1 