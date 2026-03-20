"""
Plota um gráfico 3D com paralelepípedos reto-retângulos representando as
categorias da clusterização RFM.
"""

import plotly.graph_objects as go

rfm_categories = {
    "Lost": (0, 2, 0, 2, 0, 2),
    "Loyal Customers": (2, 5, 3, 5, 3, 5),
    "Champions": (4, 5, 4, 5, 4, 5),
    "Potential Loyalist": (3, 5, 1, 3, 1, 3),
    "New Customers": (4, 5, 0, 1, 0, 1),
    "Promising": (3, 4, 0, 1, 0, 1),
    "Customers Needing Attention": (2, 3, 2, 3, 2, 3),
    "About to Sleep": (2, 3, 0, 2, 0, 2),
    "At Risk": (0, 2, 2, 5, 2, 5),
    "Can't Lose": (0, 1, 4, 5, 4, 5),
    "Hibernating": (1, 2, 1, 2, 1, 2),
}


# Cores para cada categoria
colors = {
    "Champions": "green",
    "Loyal Customers": "blue",
    "Potential Loyalist": "yellow",
    "New Customers": "purple",
    "Promising": "orange",
    "Customers Needing Attention": "pink",
    "About to Sleep": "brown",
    "At Risk": "red",
    "Can't Lose": "cyan",
    "Hibernating": "magenta",
    "Lost": "black",
}

cubes = []


def create_cube(x_min, x_max, y_min, y_max, z_min, z_max, color, name):
    # each of the 8 vertex of the cube, in order
    x = [x_min, x_max, x_max, x_min, x_min, x_min, x_max, x_max]
    y = [y_min, y_min, y_max, y_max, y_max, y_min, y_min, y_max]
    z = [z_min, z_min, z_min, z_min, z_max, z_max, z_max, z_max]

    # Definindo as faces do cubo (https://plotly.com/python/reference/mesh3d/)
    i = [
        0,
        0,
        0,
        1,
        0,
        0,
        1,
        2,
        4,
        5,
        2,
        7,
    ]
    j = [
        1,
        2,
        1,
        5,
        3,
        4,
        2,
        6,
        7,
        4,
        3,
        4,
    ]
    k = [
        2,
        3,
        5,
        6,
        4,
        5,
        6,
        7,
        6,
        6,
        7,
        3,
    ]

    # Criando o cubo
    return go.Mesh3d(x=x, y=y, z=z, i=i, j=j, k=k, color=color, opacity=0.75, name=name)


for category, (r_min, r_max, f_min, f_max, m_min, m_max) in rfm_categories.items():
    cubes.append(
        create_cube(
            x_min=r_min,
            x_max=r_max,
            y_min=f_min,
            y_max=f_max,
            z_min=m_min,
            z_max=m_max,
            color=colors[category],
            name=category,
        )
    )

# Layout do gráfico
layout = go.Layout(
    title="RFM Categorization 3D",
    scene={
        "xaxis_title": "Recency (R)",
        "yaxis_title": "Frequency (F)",
        "zaxis_title": "Monetary (M)",
        "xaxis": {"nticks": 6, "range": [0, 5]},
        "yaxis": {"nticks": 6, "range": [0, 5]},
        "zaxis": {"nticks": 6, "range": [0, 5]},
    },
    updatemenus=[
        {
            "buttons": [
                {
                    "label": category,
                    "method": "update",
                    "args": [
                        {"visible": [cat == category for cat in rfm_categories]},
                        {"title": f"RFM Categorization 3D - {category}"},
                    ],
                }
                for category in rfm_categories
            ]
            + [
                {
                    "label": "All",
                    "method": "update",
                    "args": [
                        {"visible": [True] * len(rfm_categories)},
                        {"title": "RFM Categorization 3D - All"},
                    ],
                }
            ],
            "direction": "down",
            "showactive": True,
        }
    ],
)

fig = go.Figure(data=cubes, layout=layout)

# fig.show()
fig.write_html("rfm-view.html")
