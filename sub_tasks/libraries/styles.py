th_props1 = [
    ('background-color', '#9ADCFF'),
    ('border', 'solid 1px black'),
    ('font-weight', 'bold'),
    ('text-align', 'center'),
    ('color', 'black'),
    ('padding', '.7em'),
    ('font-size', '13px')
]

th_props2 = [
    ('background-color', '#87CEEB'),
    ('border', 'solid 1px black'),
    ('font-weight', 'bold'),
    ('text-align', 'center'),
    ('color', 'black'),
    ('padding', '.7em'),
    ('font-size', '13px')
]

tr_props = [
    ('border', 'solid black 1px'),
    ('padding', '.3em')
]

td_props = [
    ('border', 'solid black 1px'),
    ('font-size', '12px'),
    ('font-variant-numeric', 'tabular-nums'),
    ('font-family', 'Georgia, serif')
]

styles = [
    dict(selector='th', props=th_props1),
    dict(selector='tr', props=tr_props),
    dict(selector='td', props=td_props)
]

styles1 = [
    dict(selector='th:nth-of-type(-n+6)', props=th_props1),
    dict(selector='th:nth-of-type(n+7)', props=th_props2),
    dict(selector='tr', props=tr_props),
    dict(selector='td', props=td_props)
]

properties = {
    "border-spacing": "0px",
    "margin-bottom": "0em",
    "text-align": "left",
    "white-space": "normal" 
}

ug_styles = [
    dict(selector='th', props=th_props2),
    dict(selector='tr', props=tr_props),
    dict(selector='td', props=td_props)
]