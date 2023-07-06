Select 
(select (case when A."U_VSPPFRX"='2' then 
(case when A."U_VSPSPCBP"='1' then 'High Rx' else 
(case when (IFNULL(A."U_VSPADRDV",'') not in ('','NONE') or IFNULL(A."U_VSPADLDV",'') not in ('','NONE')) then 'High Rx' else 
(case when ((((IFNULL(A."U_VSPSRDS",'') in ('+0.25','-0.25','0.00')) and (IFNULL(A."U_VSPSRDC",'') in ('-0.25','0.00')))
and ((IFNULL(A."U_VSPSLDS",'') in ('+0.25','-0.25','0.00')) and (IFNULL(A."U_VSPSLDC",'') in ('-0.25','0.00'))))
or (((IFNULL(A."U_VSPSRN",'') in ('+0.25','-0.25','0.00')) and (IFNULL(A."U_VSPSRNC",'') in ('-0.25','0.00')))
and ((IFNULL(A."U_VSPSLN",'') in ('+0.25','-0.25','0.00')) and (IFNULL(A."U_VSPSLNC",'') in ('-0.25','0.00'))))) 
then 'Low Rx' else 'High Rx' end) end) end) 
when A."U_VSPPFRX"='1' then 
(case when (IFNULL(A."U_VSPOLDPF",'')='1' or IFNULL(A."U_VSPOLDBF",'')='1') then 'High Rx' else 
(case when (IFNULL(A."U_VSPRXADR",'') not in ('','NONE') or IFNULL(A."U_VSPRXADL",'') not in ('','NONE')) then 'High Rx' else 
(case when ((((IFNULL(A."U_VSPRXSRD",'') in ('+0.25','-0.25','0.00')) and (IFNULL(A."U_VSPRXCRD",'') in ('-0.25','0.00')))
and ((IFNULL(A."U_VSPRXSLD",'') in ('+0.25','-0.25','0.00')) and (IFNULL(A."U_VSPRXCLD",'') in ('-0.25','0.00'))))
or (((IFNULL(A."U_VSPRXSRN",'') in ('+0.25','-0.25','0.00')) and (IFNULL(A."U_VSPRXCRN",'') in ('-0.25','0.00')))
and ((IFNULL(A."U_VSPRXSLN",'') in ('+0.25','-0.25','0.00')) and (IFNULL(A."U_VSPRXCLN",'') in ('-0.25','0.00')))))
then 'Low Rx' else 'High Rx' end) end) end)
when A."U_VSPOTSRX"='1' then
(case when (IFNULL(A."U_VSPPDNCK",'')='1' or IFNULL(A."U_VSPRGSVE",'')='1') then 'High Rx' else 
(case when (IFNULL(A."U_VSPGRADV",'') not in ('','NONE') or IFNULL(A."U_VSPGLADV",'') not in ('','NONE')) then 'High Rx' else 
(case when ((((IFNULL(A."U_VSPPGRDS",'') in ('+0.25','-0.25','0.00')) and (IFNULL(A."U_VSPPGRDC",'') in ('-0.25','0.00')))
and ((IFNULL(A."U_VSPPGLDS",'') in ('+0.25','-0.25','0.00')) and (IFNULL(A."U_VSPPGLDC",'') in ('-0.25','0.00'))))
or (((IFNULL(A."U_VSPPGRNS",'') in ('+0.25','-0.25','0.00')) and (IFNULL(A."U_VSPPGRNC",'') in ('-0.25','0.00')))
and ((IFNULL(A."U_VSPPGLNS",'') in ('+0.25','-0.25','0.00')) and (IFNULL(A."U_VSPPGLNC",'') in ('-0.25','0.00')))))
then 'Low Rx' else 'High Rx' end) end) end)
when A."U_VSPOURX"='1' then
(case when (IFNULL(A."U_VSPLSTPG",'')='1' or IFNULL(A."U_VSPLSTBF",'')='1') then 'High Rx' else 
(case when (IFNULL(A."U_VSPODRAD",'') not in ('','NONE') or IFNULL(A."U_VSPODLAD",'') not in ('','NONE')) then 'High Rx' else 
(case when ((((IFNULL(A."U_VSPODRDS",'') in ('+0.25','-0.25','0.00')) and (IFNULL(A."U_VSPODRDC",'') in ('-0.25','0.00')))
and ((IFNULL(A."U_VSPODLDS",'') in ('+0.25','-0.25','0.00')) and (IFNULL(A."U_VSPODLDC",'') in ('-0.25','0.00'))))
or (((IFNULL(A."U_VSPODRNS",'') in ('+0.25','-0.25','0.00')) and (IFNULL(A."U_VSPODRNC",'') in ('-0.25','0.00')))
and ((IFNULL(A."U_VSPODLNS",'') in ('+0.25','-0.25','0.00')) and (IFNULL(A."U_VSPODLNC",'') in ('-0.25','0.00')))))
then 'Low Rx' else 'High Rx' end) end) end) end) from "@VSP_OPT_PRES" A where A."Code"=T1."U_VSPVSID") as "Rx"

from "@VSP_OPT_ORPCT" T1 