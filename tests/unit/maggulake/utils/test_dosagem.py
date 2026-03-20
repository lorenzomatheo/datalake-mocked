import pytest

from maggulake.produtos.dosagem import extrai_dosagem_do_nome, normaliza_dosagem


class TestNormalizaDosagemFormato:
    """Testa formatação básica: espaços, vírgula decimal e separador '+'."""

    def test_none_returns_none(self):
        assert normaliza_dosagem(None) is None

    def test_empty_string_returns_none(self):
        assert normaliza_dosagem("") is None

    def test_whitespace_only_returns_none(self):
        assert normaliza_dosagem("   ") is None

    def test_sem_espaco_inalterado(self):
        assert normaliza_dosagem("500mg") == "500mg"

    def test_strip_whitespace(self):
        assert normaliza_dosagem("  500mg  ") == "500mg"

    def test_remove_espaco_numero_mg(self):
        assert normaliza_dosagem("500 mg") == "500mg"

    def test_remove_espaco_numero_mcg(self):
        assert normaliza_dosagem("200 mcg") == "200mcg"

    def test_remove_espaco_numero_percentual(self):
        assert normaliza_dosagem("0,12 %") == "0.12%"

    def test_remove_espaco_concentracao_por_volume(self):
        assert normaliza_dosagem("25 mg/mL") == "25mg/mL"

    @pytest.mark.parametrize(
        "raw, esperado",
        [
            ("10 mmol", "10mmol"),
            ("500 mui", "500mui"),
            ("2 ud", "2ud"),
            ("100 µg", "100mcg"),  # alias + remoção de espaço
        ],
    )
    def test_remove_espaco_unidades_estendidas(self, raw, esperado):
        assert normaliza_dosagem(raw) == esperado

    def test_virgula_decimal_substituida_por_ponto(self):
        assert normaliza_dosagem("12,5mg") == "12.5mg"

    def test_virgula_decimal_com_espaco(self):
        assert normaliza_dosagem("12,5 mg") == "12.5mg"

    def test_virgula_decimal_percentual(self):
        assert normaliza_dosagem("0,12%") == "0.12%"

    def test_separador_sem_espacos_normalizado(self):
        assert normaliza_dosagem("50mg+12,5mg") == "50mg + 12.5mg"

    def test_separador_com_espacos_inconsistentes_normalizado(self):
        assert normaliza_dosagem("50mg  +  12,5mg") == "50mg + 12.5mg"

    def test_multiplos_componentes(self):
        assert normaliza_dosagem("50 mg + 12,5 mg") == "50mg + 12.5mg"


class TestNormalizaDosagemGuardsEAliases:
    """Testa guards de valores inválidos, aliases de unidade e sufixos posológicos."""

    @pytest.mark.parametrize(
        "literal", ["none", "null", "n/a", "n/i", "nd", "-", "..."]
    )
    def test_literais_invalidos_retornam_none(self, literal):
        assert normaliza_dosagem(literal) is None

    def test_none_unit_nonenone_retorna_none(self):
        assert normaliza_dosagem("NoneNone") is None

    def test_none_unit_parcial_retorna_none(self):
        assert normaliza_dosagem("500None") is None

    def test_none_unit_prefixo_retorna_none(self):
        assert normaliza_dosagem("Nonemg") is None

    def test_ug_normalizado_para_mcg(self):
        assert normaliza_dosagem("200ug") == "200mcg"

    def test_unicode_ug_normalizado_para_mcg(self):
        assert normaliza_dosagem("200µg") == "200mcg"

    def test_unicode_ug_com_espaco_normalizado(self):
        assert normaliza_dosagem("200 µg") == "200mcg"

    def test_remove_sufixo_por_comprimido(self):
        assert normaliza_dosagem("500mg por comprimido") == "500mg"

    def test_remove_sufixo_por_capsula(self):
        assert normaliza_dosagem("500mg por cápsula") == "500mg"

    def test_remove_sufixo_por_dose(self):
        assert normaliza_dosagem("25mg/mL por dose") == "25mg/mL"

    def test_remove_sufixo_por_ampola(self):
        assert normaliza_dosagem("100mg por ampola") == "100mg"

    @pytest.mark.parametrize(
        "raw, esperado",
        [
            ("500mg por comprimido", "500mg"),
            ("500mg por capsula", "500mg"),
            ("500mg por cápsula", "500mg"),
            ("25mg/mL por dose", "25mg/mL"),
            ("100mg por ampola", "100mg"),
            ("200mg por sachê", "200mg"),
            ("10mg por supositório", "10mg"),
        ],
    )
    def test_sufixos_por_parametrizados(self, raw, esperado):
        assert normaliza_dosagem(raw) == esperado


class TestNormalizaDosagemNovasOtimizacoes:
    """Testa otimizações 1-3: milhar, /gota+/cepa e sufixos posológicos estendidos."""

    # 1. Separador de milhar
    def test_milhar_simples(self):
        assert normaliza_dosagem("1.000mg") == "1000mg"

    def test_milhar_duplo(self):
        assert normaliza_dosagem("1.000.000ui") == "1000000ui"

    def test_milhar_com_unidade_por_volume(self):
        assert normaliza_dosagem("100.000ui/ml") == "100000ui/ml"

    def test_milhar_nao_confunde_decimal(self):
        # "1,5mg" → ponto decimal, não milhar
        assert normaliza_dosagem("1,5mg") == "1.5mg"

    def test_milhar_preserva_decimal_com_zero_inicial(self):
        # "0.125mg" → líder 0 não pode ser prefixo de milhar
        assert normaliza_dosagem("0.125mg") == "0.125mg"

    def test_milhar_preserva_decimal_ambiguo_1250(self):
        # "1.250mg" → ambíguo; preservado para não inflar dosagem
        assert normaliza_dosagem("1.250mg") == "1.250mg"

    def test_milhar_preserva_decimal_ambiguo_10250(self):
        # "10.250mg" → ambíguo; preservado para não inflar dosagem
        assert normaliza_dosagem("10.250mg") == "10.250mg"

    # 2. Denominadores de dispensação (/gota, /cepa)
    def test_remove_denominador_gota(self):
        assert normaliza_dosagem("0,21mg/gota") == "0.21mg"

    def test_remove_denominador_gota_sem_decimal(self):
        assert normaliza_dosagem("0,5mg/gota") == "0.5mg"

    def test_remove_denominador_cepa(self):
        assert normaliza_dosagem("15mcg/cepa") == "15mcg"

    def test_remove_denominador_cepa_com_volume(self):
        # "/cepa/0,5ml" → tudo após /cepa é removido
        assert normaliza_dosagem("15mcg/cepa/0,5ml") == "15mcg"

    # 3. Sufixos posológicos estendidos
    def test_remove_sufixo_por_porcao(self):
        assert normaliza_dosagem("1500mg por porção") == "1500mg"

    def test_remove_sufixo_por_dia(self):
        assert normaliza_dosagem("500mg por dia") == "500mg"

    def test_remove_sufixo_por_gota(self):
        assert normaliza_dosagem("0,5mg por gota") == "0.5mg"

    def test_remove_sufixo_por_quilo(self):
        assert normaliza_dosagem("0,25mg por quilo de peso corpóreo") == "0.25mg"

    def test_remove_sufixo_por_kg(self):
        assert normaliza_dosagem("0,1g por kg de peso corporal") == "0.1g"

    def test_remove_sufixo_para_cada_cepa(self):
        assert normaliza_dosagem("15mcg para cada cepa") == "15mcg"

    def test_remove_sufixo_para_cada_sorotipo(self):
        assert normaliza_dosagem("2,2mcg para cada sorotipo") == "2.2mcg"


class TestNormalizaDosagemDescritoresETemporais:
    """Testa otimizações 4-5: descritores de ingrediente e sufixos temporais."""

    # 4. Descritores de ingrediente embutidos
    def test_remove_descritor_vitamina_c(self):
        assert normaliza_dosagem("1g de vitamina c por cápsula") == "1g"

    def test_remove_descritor_cafeina(self):
        assert normaliza_dosagem("200mg de cafeína por cápsula") == "200mg"

    def test_remove_descritor_omega(self):
        assert normaliza_dosagem("1000mg de ômega 3 por cápsula") == "1000mg"

    def test_remove_descritor_hemaglutinina(self):
        # _DENOMINADOR_SLASH_RE remove /cepa; _DE_INGREDIENTE_RE remove "de hemaglutinina"
        assert normaliza_dosagem("15mcg de hemaglutinina/cepa") == "15mcg"

    def test_remove_descritor_composto_preserva_ambos(self):
        assert (
            normaliza_dosagem("200mg de cafeína + 300mg de taurina") == "200mg + 300mg"
        )

    # 5. Sufixos temporais
    def test_remove_temporal_por_hora(self):
        assert normaliza_dosagem("10mcg/h") == "10mcg"

    def test_remove_temporal_por_24h(self):
        assert normaliza_dosagem("13,3mg/24h") == "13.3mg"

    def test_remove_temporal_virgula_12h(self):
        assert normaliza_dosagem("150mg,12h") == "150mg"

    def test_remove_temporal_parentetico(self):
        assert normaliza_dosagem("9,0mg (4,6mg/24h)") == "9.0mg"


class TestNormalizaDosagemTerceiraRodada:
    """Testa otimizações 11-15: unidades especiais, intervalos, frações, adimensionais e escala."""

    # 11. Unidades especializadas → None
    def test_ufp_retorna_none(self):
        assert normaliza_dosagem("1.350 ufp") is None

    def test_tru_retorna_none(self):
        assert normaliza_dosagem("25tru/g") is None

    def test_lf_retorna_none(self):
        assert normaliza_dosagem("15 lf toxoide") is None

    def test_ud_retorna_none(self):
        assert normaliza_dosagem("40 u.d. poliovírus") is None

    def test_ch_homeopatico_retorna_none(self):
        assert normaliza_dosagem("1ch") is None

    def test_d_homeopatico_retorna_none(self):
        assert normaliza_dosagem("1d") is None

    def test_u_bare_sem_espaco_normalizado_para_ui(self):
        assert normaliza_dosagem("10000u") == "10000ui"

    def test_u_bare_com_espaco_normalizado_para_ui(self):
        assert normaliza_dosagem("100 u") == "100ui"

    # 12. Intervalos e alternativas → None
    def test_hifen_range_retorna_none(self):
        assert normaliza_dosagem("100mg - 150mg") is None

    def test_hifen_compacto_retorna_none(self):
        assert normaliza_dosagem("15-19mg/ml") is None

    def test_ou_alternativa_retorna_none(self):
        assert normaliza_dosagem("100mg ou 1g") is None

    def test_a_range_retorna_none(self):
        assert normaliza_dosagem("0.1 a 0.25 g/kg") is None

    def test_virgula_lista_retorna_none(self):
        assert normaliza_dosagem("100mg, 200mg") is None

    # 13. Frações complexas → None
    def test_fracao_sem_unidade_numerador_retorna_none(self):
        assert normaliza_dosagem("20/30mg") is None

    def test_fracao_multipla_retorna_none(self):
        assert normaliza_dosagem("100/25mg") is None

    # 14. Razões adimensionais → None
    def test_ml_por_ml_retorna_none(self):
        assert normaliza_dosagem("0,07ml/ml") is None

    def test_mg_por_mg_retorna_none(self):
        assert normaliza_dosagem("0,4mg/mg") is None

    # 15. Mistura de escalas → None
    def test_mistura_mg_e_g_retorna_none(self):
        assert normaliza_dosagem("10mg + 3,5g + 12g") is None

    def test_mg_por_g_legitimo_nao_afetado(self):
        # mg/g é unidade de concentração válida — não deve ser rejeitado
        assert normaliza_dosagem("0.5mg/g") == "0.5mg/g"


class TestNormalizaDosagemSegundaRodada:
    """Testa otimizações 6-10: instruções, parentéticos, comparação, expressões e científico."""

    # 6. Texto descritivo longo (instrução → None)
    def test_instrucao_comprimido_por_dia_retorna_none(self):
        assert normaliza_dosagem("1 comprimido por dia") is None

    def test_instrucao_comprimidos_a_cada_8h_retorna_none(self):
        assert normaliza_dosagem("2 comprimidos a cada 8 horas") is None

    def test_instrucao_range_retorna_none(self):
        assert normaliza_dosagem("1 a 2 comprimidos até 4 vezes ao dia") is None

    def test_dosagem_com_comprimido_nao_e_instrucao(self):
        # "500mg por comprimido" começa com dose — não é instrução
        assert normaliza_dosagem("500mg por comprimido") == "500mg"

    # 7. Parentéticos descritivos
    def test_remove_parentetico_drag_a(self):
        assert normaliza_dosagem("0,05mg + 0,03mg (drag a)") == "0.05mg + 0.03mg"

    def test_remove_parentetico_com_branco(self):
        assert normaliza_dosagem("0,125mg (com branco)") == "0.125mg"

    def test_remove_parentetico_ampola(self):
        assert normaliza_dosagem("100mg (ampola 1) + 5mg (ampola 2)") == "100mg + 5mg"

    # 8. Operadores de comparação
    def test_remove_prefixo_maior_igual(self):
        assert normaliza_dosagem("≥ 1.000ui") == "1000ui"

    def test_remove_prefixo_maior(self):
        assert normaliza_dosagem("> 20 ui") == "20ui"

    # 9. Expressões matemáticas (parênteses de agrupamento)
    def test_distribui_unidade_fora_de_parenteses(self):
        assert normaliza_dosagem("(1000 + 200)mg") == "1000mg + 200mg"

    def test_distribui_unidade_fora_tres_componentes(self):
        assert (
            normaliza_dosagem("(120+120+120)mcg/ml")
            == "120mcg/ml + 120mcg/ml + 120mcg/ml"
        )

    def test_remove_parenteses_de_agrupamento(self):
        assert (
            normaliza_dosagem("(0,05mg + 0,03mg) + (0,075mg + 0,04mg)")
            == "0.05mg + 0.03mg + 0.075mg + 0.04mg"
        )

    # 10. Notação exponencial / científica → None
    def test_notacao_ccid50_retorna_none(self):
        assert normaliza_dosagem("10^3,0 ccid50") is None

    def test_notacao_gv_ml_retorna_none(self):
        assert normaliza_dosagem("2,0 x 10e13 gv/ml") is None


class TestExtraiDosagemDoNome:
    def test_none_returns_none(self):
        assert extrai_dosagem_do_nome(None) is None

    def test_empty_string_returns_none(self):
        assert extrai_dosagem_do_nome("") is None

    def test_nome_sem_dosagem_returns_none(self):
        assert extrai_dosagem_do_nome("Shampoo Dove") is None

    def test_ml_sozinho_nao_e_dosagem(self):
        assert extrai_dosagem_do_nome("Shampoo Dove 400ml") is None

    def test_gramas_sozinho_nao_e_dosagem(self):
        assert extrai_dosagem_do_nome("Pomada Cicatrizante 10g") is None

    def test_extrai_mg_simples(self):
        assert extrai_dosagem_do_nome("Dipirona 500mg Comprimido") == "500mg"

    def test_extrai_mg_com_espaco(self):
        assert extrai_dosagem_do_nome("Paracetamol 750 mg Comprimido") == "750mg"

    def test_extrai_percentual(self):
        assert extrai_dosagem_do_nome("Hidrocortisona 1% Creme") == "1%"

    def test_extrai_percentual_decimal(self):
        assert extrai_dosagem_do_nome("Clorexidina 0,12% Solucao") == "0,12%"

    def test_extrai_mcg(self):
        assert extrai_dosagem_do_nome("Budesonida 200mcg Spray") == "200mcg"

    def test_extrai_ui_por_ml(self):
        assert extrai_dosagem_do_nome("Insulina 100 UI/mL") == "100UI/mL"

    def test_extrai_mg_por_ml(self):
        assert extrai_dosagem_do_nome("Dipirona Sodica 500mg/ml Gotas") == "500mg/ml"

    def test_extrai_mg_por_volume_numerico(self):
        assert extrai_dosagem_do_nome("Amoxicilina 500mg/5mL Suspensao") == "500mg/5mL"

    def test_extrai_primeira_ocorrencia(self):
        # Quando há dois padrões, extrai o primeiro
        assert (
            extrai_dosagem_do_nome("Losartana 50mg + Hidroclorotiazida 12,5mg")
            == "50mg"
        )

    @pytest.mark.parametrize(
        "nome, esperado",
        [
            ("Amoxicilina 500mg Capsula", "500mg"),
            ("Losartana 50mg Comprimido", "50mg"),
            ("Omeprazol 20mg Capsula", "20mg"),
            ("Atenolol 25mg Comprimido", "25mg"),
            ("Fluoxetina 20mg Capsula", "20mg"),
        ],
    )
    def test_medicamentos_comuns(self, nome, esperado):
        assert extrai_dosagem_do_nome(nome) == esperado
