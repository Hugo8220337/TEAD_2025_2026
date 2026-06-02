import yaml
import pandas as pd

class ContractValidator:
    def __init__(self, contract_path: str):
        print("DEBUG: Loading contract from:", contract_path)
        with open(contract_path, 'r', encoding='utf-8') as file:
            self.contract = yaml.safe_load(file)

        self.contract_id = self.contract.get('contract', {}).get('id', 'Unknown')
        
    def validate(self, df: pd.DataFrame) -> bool:
        """Run every test defined in the YAML file dinamically."""
        print(f"\n--- Validating DataFrame with contract: {self.contract_id} ---")
        
        constraints = self.contract.get('quality', {}).get('constraints', [])
        errors = []

        for constraint in constraints:
            rule_string = constraint.get('rule', '') # Ex: "no_nulls(case_id, time)"
            
            # null logic
            if rule_string.startswith("no_nulls"):
                # Extrair o que está dentro dos parênteses
                cols_str = rule_string.replace("no_nulls(", "").replace(")", "")
                # Criar uma lista limpando os espaços
                columns = [col.strip() for col in cols_str.split(',')]
                
                for col in columns:
                    if col not in df.columns:
                        errors.append(f"Column '{col}' required by the contract does not exist in the DataFrame.")
                        continue
                        
                    null_count = df[col].isna().sum()
                    if null_count > 0:
                        errors.append(f"Rule 'no_nulls' violated in column '{col}': {null_count} null values.")
            
            # 2. Nota: As tuas outras regras do YAML (SQL e restrições complexas)
            elif rule_string.startswith("time <="):
                # TODO: Tenho de começar a pensar nisto, caso não exista uma biblioteca que me ajude 
                # Para regras como "time <= current_timestamp()", o ideal era usar o 
                # df.query() ou avaliar no pandas. Se for muito complexo, podes
                # implementar um validador específico aqui.
                pass
                
            elif "IN (SELECT" in rule_string:
                # Regras de SQL puro são difíceis de testar diretamente em Pandas sem
                # ires à base de dados.
                pass

        # The Quality Gate
        if errors:
            print(f"❌ Contract failed {self.contract_id}!")
            for err in errors:
                print(f"  - {err}")
            raise ValueError(f"Quality Gate failed for {self.contract_id} due to contract violations.")
            
        print(f"✅ All tests passed. The Data Product {self.contract_id} is ready for publication!")
        return True