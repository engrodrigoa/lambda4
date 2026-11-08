-- Produto ANP (chave natural: o nome publicado). Unidade e ordem de exibição vêm da seed.
select
    produto,
    product_group,
    unidade_medida,
    ordem,
    replace(replace(initcap(produto), 'Gnv', 'GNV'), 'Glp', 'GLP')  as produto_rotulo,
    product_group = 'glp'                                as fl_glp
from {{ ref('anp_produto_grupo') }}
