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

th_props3 = [
    ('background-color', '#A7C7E7'),
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

styles_daily = [
    dict(selector='th', props=th_props3),
    dict(selector='tr', props=tr_props),
    dict(selector='td', props=td_props)
]



bi_th = [
    ('background-color', '#ADD8E6'),  # Light blue for a softer look
    ('border', 'solid 1px #2F4F4F'),  # Dark slate gray border
    ('font-weight', 'bold'),
    ('text-align', 'center'),
    ('color', 'black'),
    ('padding', '.7em'),
    ('font-size', '13px')
]

bi_tr = [
    ('border', 'solid #A9A9A9 1px'),  # Light gray border
    ('padding', '.3em')
]

bi_td = [
    ('border', 'solid black 1px'),  # Light gray border
    ('font-size', '12px'),
    ('font-variant-numeric', 'tabular-nums'),
    ('font-family', 'Georgia, serif')
]

# Additional styles to improve table aesthetics


bi_caption = [
    ('caption-side', 'bottom'),
    ('text-align', 'right'),
    ('font-size', '12px'),
    ('color', '#6A5ACD')  # Slate blue color for a nice touch
]

bi_hover = [
    ('&:hover', [
        ('background-color', '#F0F8FF')  # Alice blue on hover
    ])
]

bi_weekly = [
    dict(selector='th', props=bi_th),
    dict(selector='tr', props=bi_tr),
    dict(selector='td', props=bi_td),
]
