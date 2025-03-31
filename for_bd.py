import re
import pandas as pd
import ipaddress
from itertools import zip_longest

def simplify_ip(ip_list):
    if len(ip_list) > 1:
        return "many"
    
    split_ips = [ip.split('.') for ip in map(str, ip_list)]
    transposed = list(zip_longest(*split_ips, fillvalue=None))
    simplified_ip = [
        next(filter(None, segment), '*') if len(set(filter(None, segment))) == 1 else '*'
        for segment in transposed[:4]
    ]
    return '.'.join(simplified_ip)


def contains_test(text):
    text = str(text).lower()
    text = text.replace('-', '').replace('_', '')
    pattern = r'test'
    return bool(re.search(pattern, text))


def is_private(ip):
                try:
                    ip_obj = ipaddress.ip_address(ip)
                    return ip_obj.is_private
                except ValueError:
                    return False

def proverka(df):
                df_win = pd.read_excel('list_win.xlsx')
                df_lin = pd.read_excel('list_lin.xlsx')
    
                result_win = set(df_win.iloc[:, 0].dropna().astype(str))
                result_lin = set(df_lin.iloc[:, 0].dropna().astype(str))
                
                return result_win, result_lin


def extract_valid_servers_win(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', encoding='utf-8') as outfile:
        server_name = None
        for line in infile:
            
            server_match = re.search(r'(.*?)\|', line)
            if server_match:
                server_name = server_match.group(1).strip()
                continue
            
            
            parts = line.split()
            if len(parts) > 2 and ":" in parts[2] and ":" in parts[1]:
                foreign_address = parts[2]
                local_address = parts[1]  
                state = parts[3]
                
                
                if not (foreign_address.startswith("0.0.0.0") or
                        foreign_address.startswith("localhost") or
                        foreign_address.startswith("[::]") or
                        local_address.startswith("0.0.0.0") or
                        local_address.startswith("localhost") or
                        local_address.startswith("[::]") or not ( state.startswith("ESTABLISHED"))):
                    
                    ip_port_pair = foreign_address.split(":")
                    if len(ip_port_pair) == 2:  
                        ip, port = ip_port_pair
                        
                        outfile.write(f"{server_name} {ip} {port} {state}\n")



def extract_valid_servers_lin(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', encoding='utf-8') as outfile:
        server_name = None
        for line in infile:
            server_match = re.search(r'=== Output from (.+) ===', line)
            if server_match:
                server_name = server_match.group(1)
                continue
            
            if server_name:
                parts = line.split()
                if len(parts) > 2 and ":" in parts[3] and ":" in parts[4]:

                    proc, local_address, foreign_address, State, User, PIDProgram_name,PIDProgram_name_con   =(parts[0], parts[3], parts[4], parts[5],parts[6],  parts[8] if len(parts) > 8 else '-',parts[9] if len(parts)>9 else ''     )           
                    
                    if not (foreign_address.startswith("0.0.0.0") or 
                            foreign_address.startswith("localhost") or 
                            foreign_address.startswith("[::]") or local_address.startswith("0.0.0.0") or 
                            local_address.startswith("localhost") or 
                            local_address.startswith("[::]") or not ( State.startswith("ESTABLISHED"))):
                        # Записываем оба адреса в одну строку
                        ip, port = foreign_address.rsplit(":", 1)
                        
                        outfile.write(f"{server_name} {ip} {port} {State}\n")


def compare_and_update_excel(file_path1, file_path2):
                try:
                    
                    df1 = pd.read_excel(file_path1)
                    df2 = pd.read_excel(file_path2)

                   
                    required_columns = ['Server_name', 'IP', 'Dest']
                    if not all(col in df1.columns for col in required_columns):
                        raise ValueError("В первом файле отсутствуют необходимые колонки")
                    if not all(col in df2.columns for col in required_columns):
                        raise ValueError("Во втором файле отсутствуют необходимые колонки")

                    
                    df1['compare_key'] = df1.apply(lambda x: str(x['Server_name']) + 
                                                str(x['IP']) + 
                                                str(x['Dest']), axis=1)
                    df2['compare_key'] = df2.apply(lambda x: str(x['Server_name']) + 
                                                str(x['IP']) + 
                                                str(x['Dest']), axis=1)

                   
                    mask = ~df2['compare_key'].isin(df1['compare_key'])
                    new_records = df2[mask].drop('compare_key', axis=1)

                    
                    if len(new_records) > 0:
                        updated_df = pd.concat([df1.drop('compare_key', axis=1), 
                                            new_records], ignore_index=True)
                        
                    
                        updated_df.to_excel(file_path1, index=False)
                        print(f"Добавлено {len(new_records)} новых записей")
                    else:
                        print("Новые записи не найдены")

                except Exception as e:
                    print(f"Произошла ошибка: {str(e)}")



def convert_network_log_to_excel(input_file, output_file):
    data_rows = []
    with open(input_file, 'r', encoding='utf-8') as file:
        next(file)
        next(file)
            
        for line in file:
            parts = line.strip().split()     
            if parts:
                server_name = parts[0]
                
                local_addr = parts[1]
                foreign_addr = parts[2]
                
                pid_program = ' '.join(parts[5:]) if len(parts) > 5 else ''
                
                data_rows.append({
                    'Server_name': server_name,
                    'IP': local_addr,
                    'Port': foreign_addr
                })
    
    df = pd.DataFrame(data_rows)

    result_dict = {}

    for _, row in df.iterrows():
        server_name = row['Server_name']
        ip = row['IP']
        port = str(row['Port'])  
        
       
        if ip in result_dict:
            ports = result_dict[ip].split(',')
            if port not in ports:
                result_dict[ip] += f',{port}'
        else:
            result_dict[ip] = f"{server_name}\t{ip}\t{port}"
    
    result_rows = []
    for ip, value in result_dict.items():
        server_name, _, ports = value.split('\t')
        result_rows.append([server_name, ip, ports])
    
    df1 = pd.DataFrame(result_rows, columns=['Server_name', 'IP', 'Port'])
    #df1.to_excel('Serv_IP_PORT.xlsx', index=False)
    df_linux = pd.read_excel('list_lin.xlsx')
    df_wind = pd.read_excel('list_win.xlsx')

    try:

        df_hosts = pd.concat([
            df_linux[['IP', 'Name']].rename(columns={'Name': 'host2'}), 
            df_wind[['IP', 'Host']].rename(columns={'Host': 'host3'})
        ], axis=0)

        merged_df = pd.merge(
            df1[['Server_name', 'IP']], 
            df_hosts,
            on='IP',
            how='left'
        )
        
        
        merged_df['Dest'] = merged_df['host2'].fillna(merged_df['host3'])
        
        merged_df['Dest'] = merged_df['Dest'].fillna('Что-то снаружи')
        
        merged_df.loc[(merged_df['Dest'] == 'Что-то снаружи') & (merged_df['IP'].astype(str).str.startswith('10.20.40')),'Dest'] = 'IT PC'
        
        df = merged_df.drop(['host2', 'host3'], axis=1)

        try:
        

            df['IP_source'] = '-'
            df['Group_by_dest'] = 'Prod'
     
            linux_ips = dict(zip(df_linux['Name'].str.lower(), df_linux['IP']))
            windows_ips = dict(zip(df_wind['Host'].str.lower(), df_wind['IP']))

            def get_ip(server_n):
                serv_name_lower = server_n.lower()
                linux_ip = linux_ips.get(serv_name_lower)
                windows_ip = windows_ips.get(serv_name_lower)
                return linux_ip if linux_ip is not None else windows_ip

            df['IP_source'] = df['Server_name'].apply(get_ip)

            df_linux = pd.read_excel('list_lin.xlsx', header=None, names=['IP'])
            df_windows = pd.read_excel('list_win.xlsx', header=None, names=['IP'])
            df_it = pd.read_excel('it.xlsx', header=None, names=['IP'])

            linux_set = set(df_linux['IP'].str.strip())
            windows_set = set(df_windows['IP'].str.strip())
            iti_set = set(df_it['IP'].str.strip())
                            
            lin_set = {ip.strip() for ip in linux_set}
            win_set = {ip.strip() for ip in windows_set}    
            it_set = {ip.strip() for ip in iti_set} 
            
            df['NT_Group_by_dest'] = '-'
            for row in df.itertuples():
                ip_list = str(row.IP).split(",")
                for ip in ip_list:
                    ip = ip.strip()
                    if row.Dest == "Что-то снаружи":
                        if is_private(ip):
                                df.at[row.Index, 'Dest'] = 'LAN'
                        elif ip in lin_set:
                                df.at[row.Index, 'Dest'] = 'LS'
                        elif ip in win_set:
                                df.at[row.Index, 'Dest'] = 'WS'
                        elif ip in it_set:
                                df.at[row.Index, 'Dest'] = 'IT'
                        else:
                                df.at[row.Index, 'Dest'] = 'WAN'  

            df['Group_by_serv'] = 'Prod'
            


            general = pd.read_excel("general.xlsx")["server_name"].to_list()
            general_group_source = df['Server_name'].isin(general)
            general_group_dest = df['Dest'].isin(general)
            df.loc[general_group_source, 'Group_by_serv'] = 'General'
            df.loc[general_group_dest, 'Group_by_dest'] = 'General'


            ip_it = pd.read_excel('it.xlsx')['server_name'].tolist()
            it_usr = df['Server_name'].isin(ip_it)
            df.loc[it_usr, 'Group_by_serv'] = 'IT'
            
            it_dest = df['Dest'].isin(ip_it)
            df.loc[it_dest, 'Group_by_dest'] = 'IT'
            
            ip_it_des = df['IP'].fillna('').astype(str).str.startswith("10.20.40")
            df.loc[ip_it_des,'Group_by_dest'] =  'IT'
            
            ip_it_dest = df['IP'].fillna('').astype(str).str.startswith(("172.20.50", "172.20.51"))
            df.loc[ip_it_dest,'Dest'] =  'IP TEL'
            

            df.loc[df['Dest'] == 'IT PC', 'Group_by_dest'] = 'IT'

            
           

            test_usr = df['Server_name'].apply(contains_test)
            test_usr_des = df['Dest'].apply(contains_test)

            df.loc[test_usr, 'Group_by_serv'] = 'Test'
            df.loc[test_usr_des, 'Group_by_dest'] = 'Test'

            
            arhive = pd.read_csv('arhiv.csv')['IP'].tolist()
            archive = df['IP_source'].isin(arhive)
            df.loc[archive, 'Group_by_serv'] = 'Archive'

            archive_des = df['IP'].isin(arhive)
            df.loc[archive_des, 'Group_by_dest'] = 'Archive'


            df['Group_by_serv'] = df['Group_by_serv'].fillna('Prod')
            df.to_excel('test_test_1.xlsx')

            result_win, result_lin = proverka(df)

            result_lin_lower = [name.lower() for name in result_lin]
            result_win_lower = [name.lower() for name in result_win]

            df['OS_Serv'] = df['Server_name'].apply(
                lambda x: 'Linux' if x.lower() in result_lin_lower else ('Windows' if x.lower() in result_win_lower else '-')
            )

            df['OS_Dest'] = df['Dest'].apply(
                lambda x: 'Linux' if x.lower() in result_lin_lower else ('Windows' if x.lower() in result_win_lower else '-')
            )
            df.loc[df['Dest'] == 'IT PC', 'OS_Dest'] = 'Windows'
            
            df.to_excel(output_file, index=False)
                       
        except FileNotFoundError:
            print("Ошибка: Входной файл не найден")
        except Exception as e:
            print(f"Произошла ошибка при обработке файла: {str(e)}")
        
    except Exception as e:
        print(f"Произошла ошибка: {str(e)}")


extract_valid_servers_win('res.txt', 'new_logs_WINDOWS.txt')
extract_valid_servers_lin('netstat.txt', 'new_logs_LINUX.txt')

files = ['new_logs_WINDOWS.txt', 'new_logs_LINUX.txt']

with open ('logs_for_work_merged.txt', 'w' ) as fil:
    for file in files:
        with open (file) as infile:
            fil.write(infile.read())
 
convert_network_log_to_excel('logs_for_work_merged.txt','bd.xlsx')