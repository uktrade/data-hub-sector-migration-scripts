import copy

import click
import numpy as np, pandas as pd


def create_migration_object(old_sector, new_sector):
    old_sector_split = old_sector.split(' : ')
    sector1 = {
        'path': old_sector,
        'level': len(old_sector_split),
    }
    
    new_sector_split = new_sector.split(' : ')
    sector2 = {
        'path': new_sector,
        'level': len(new_sector_split),
    }
    return {
        'old_sector': sector1,
        'new_sector': sector2
    }

def create_parents(sector_path, new_sectors_ids, new_sectors_cluster_ids):
    parents = sector_path.split(' : ')[:-1]
    for i, parent in enumerate(parents):
        parent_path = ' : '.join(parents[:i+1])
        if not sector_exists(parent_path):
            print('create_sector:', parent_path)
            id = (
                new_sectors_ids[parent_path]
                if parent_path in new_sectors_ids
                else None
            )
            sector_cluster_id = (
                new_sectors_cluster_ids[parent_path]
                if parent_path in new_sectors_cluster_ids
                else None
            )
            create_sector(
                parent_path,
                id=id,
                sector_cluster_id=sector_cluster_id
            )

def create_sector(sector_path, id=None, sector_cluster_id=None):
    if not sector_exists(sector_path):
        print('create_sector:', sector_path)
        sectors = state['sectors']
        sectors_to_create = state['sectors_to_create']
        
        segment = sector_path.split(' : ')[-1]
        parents = sector_path.split(' : ')[:-1]
        parent_path = ' : '.join(parents)
        parent = get_sector(parent_path) if parent_path != '' else None
        parent_id = parent['id'] if parent is not None else None

        if id is None:
            raise Exception('test')
            while id is None or len([s for s in sectors if s['id'] == id]) > 0:
                id = uuid.uuid4()

        if sector_cluster_id is None:
            sector_cluster_id = parent['sector_cluster_id'] if parent else None

        sector = {
            'id': str(id), 
            'path': sector_path, 
            'parent_id': parent_id, 
            'sector_cluster_id': sector_cluster_id,
            'segment': segment
        }

        sectors.append(sector)
        sectors_to_create.append(sector)

def delete_sector(sector_path):
    print('delete_sector:', sector_path)
    sectors = state['sectors']
    sectors_to_delete = state['sectors_to_delete']
    sector = [s for s in sectors if s['path'] == sector_path][0]
    sector['to_delete'] = True
    sectors_to_delete.append(sector)
    # sectors_to_delete.append((sector['id'], sector_path))

def generate_sectors_to_create_csvs(sectors):
    # columns=['id', 'segment', 'sector_cluster_id', 'parent_id', 'path']
    fields = ['id', 'segment', 'sector_cluster_id', 'parent_id', 'path']
    data = []
    for sector in sectors:
        data.append([sector[field] for field in fields])
    df = pd.DataFrame(data, columns=fields)
    df.to_csv('sectors_to_create.csv', index=False)
    
    df = df.sort_values('path', ascending=False)
    df.to_csv('sectors_to_create_reversed.csv', index=False)

def generate_sectors_to_rename_or_adopt(sectors):
    fields_base = [
        'id',
        'segment',
        'parent_id',
        'path',
        'sector_cluster_id',
    ]
    fields_old = ['old_' + field for field in fields_base]
    fields_new = ['new_' + field for field in fields_base]
    fields = fields_old + fields_new
    data = []
    for old_sector, new_sector in sectors:
        d_old = [old_sector[field] for field in fields_base]
        d_new = [new_sector[field] for field in fields_base]
        d = d_old + d_new
        data.append(d)
        
    df = pd.DataFrame(data, columns=fields)
    df.to_csv('sectors_to_rename_or_adopt.csv', index=False)
    
    df = df.sort_index(ascending=False)
    fields_swap = fields_new + fields_old
    df.columns = fields_swap
    df = df[fields]
    df.to_csv('sectors_to_rename_or_adopt_reversed.csv', index=False)

def generate_sectors_to_migrate(sectors):
    fields_base = ['path', 'id']
    fields_src = ['sector_src', 'sector_src_id']
    fields_dest = ['sector_dest', 'sector_dest_id']
    fields = fields_src + fields_dest

    data = []
    for sector_src, sector_dest in sectors:
        d_src = [sector_src[field] for field in fields_base]
        d_dest = [sector_dest[field] for field in fields_base]
        d = d_src + d_dest
        data.append(d)

    df = pd.DataFrame(data, columns=fields)
    df.to_csv('sectors_to_migrate.csv', index=False)

    fields_swap = fields_dest + fields_src
    df.columns = fields_swap
    df = df[fields]
    df.to_csv('sectors_to_migrate_reversed.csv', index=False)

def generate_sectors_to_delete(sectors):
    fields = ['id', 'path', 'parent_id', 'sector_cluster_id']
    data = []
    for sector in sectors:
        data.append([sector[field] for field in fields])
    df = pd.DataFrame(data, columns=fields)
    df.to_csv('sectors_to_delete.csv', index=False)

    df = df.sort_values('path', ascending=False)
    df.to_csv('sectors_to_delete_reversed.csv', index=False)
    
def get_sector(sector_path):
    sectors = state['sectors']
    matches = [s for s in sectors if s['path'] == sector_path]
    assert len(matches) < 2, 'Duplicate sectors found' \
        f'{[m["path"] for m in matches]}'
    return matches[0] if len(matches) == 1 else None
            
def load_df_create_sector_ids(filepath):
    df = pd.read_csv(filepath)
    return df

def load_df_existing_dh_sectors(filepath):
    print('load_df_existing_dh_sectors: ', end='')

    def get_parents(sector, sectors, parents=None):
        parents = parents if parents is not None else []
        if sector['parent_id'] is None:
            return parents
        else:
            parent = [s for s in sectors if s['id'] == sector['parent_id']][0]
            return get_parents(parent, sectors, parents=[parent] + parents)
    
    
    df = pd.read_csv(filepath)
    df['parent_id'] = df['parent_id'].replace({np.nan: None})
    sectors = df.to_dict(orient='records')
    sectors_ = []
    for sector in sectors:
        s = {
            'id': sector['id'],
            'parent_id': sector['parent_id'],
            'sector_cluster_id': sector['sector_cluster_id'],
            'segment': sector['segment']
        }
        sectors_.append(s)

    for s in sectors_:
        parents = get_parents(s, sectors_)
        s['path'] = ' : '.join([p['segment'] for p in parents] + [s['segment']])


    df = pd.DataFrame(sectors_)

    # check: duplicates
    df_ = df.groupby('path').size()
    df_ = df_.reset_index()
    df_ = df_[df_[0] > 1]
    df_ = pd.merge(df_, df, on='path')
    assert len(df_) == 0, '\nDuplicate paths found:\n{df_}'

    # check: ids
    df_ = df[df['id'].isna()]
    assert len(df_) == 0, '\nMissing ids:\n{df_}'

    print('OK')
    return df

def load_df_final_sector_list(filepath):
    print('load_df_final_sector_list:', end='')
    df = pd.read_csv(filepath)

    # check unique sectors
    df_ = df.groupby('sector').size()
    df_ = df_.reset_index()
    df_ = df_[df_[0] > 1]
    df_ = pd.merge(df_, df, on='sector')
    assert len(df_) == 0, f'\nDuplicate sectors found:\n{df_}'
    print(' OK')
    
    return df

def load_df_sector_mappings(filepath):
    print('load_df_sector_mappings: ', end='')
    df = pd.read_csv(filepath)
    df = df[df['disable'].isna()]

    # check unique sector mapping
    df_ = df.groupby('old_sector').size()
    df_ = df_.reset_index()
    df_ = df_[df_[0] > 1]
    df_ = pd.merge(df, df_, on='old_sector')
    assert len(df_) == 0, f'\nDuplicate mappings found\n{df_}'
    print('OK')
    
    return df
    
def migrate_data(sector_path_src, sector_path_dest):
    print(f'migrate_data: {sector_path_src} -> {sector_path_dest}')
    sectors = state['sectors']
    sectors_to_migrate = state['sectors_to_migrate']
    
    sector_src = get_sector(sector_path_src)
    sector_dest = get_sector(sector_path_dest)

    sectors_to_migrate.append((sector_src, sector_dest))

def sector_exists(sector_path):
    return len([s for s in state['sectors'] if s['path'] == sector_path]) > 0

def to_migration_object(old_sector, new_sector):
    old_sector_split = old_sector.split(' : ')
    sector1 = {
        'path': old_sector,
        'level': len(old_sector_split),
    }
    
    new_sector_split = new_sector.split(' : ')
    sector2 = {
        'path': new_sector,
        'level': len(new_sector_split),
    }
    return {
        'old_sector': sector1,
        'new_sector': sector2
    }

def update_sector_path(old_path, new_path):
    migrations = state['migrations']
    sectors = state['sectors']
    sectors_to_rename_or_adopt = state['sectors_to_rename_or_adopt']

    sector = get_sector(old_path)
    old_sector = copy.deepcopy(sector)
    sector['path'] = new_path
     
    # update migration objects
    for i, migration in enumerate(migrations):
        if migration['old_sector']['path'].startswith(old_path):
            migration['old_sector']['path'] = new_path + \
                migration['old_sector']['path'][len(old_path):]
    
    # update sectors
    for i, s in enumerate(sectors):
        if s['path'].startswith(old_path):
            s['path'] = new_path + s['path'][len(old_path):]
    
    id = sector['id']
    old_name = sector['segment']
    new_name = new_path.split(' : ')[-1]
    sector_cluster_id = sector['sector_cluster_id']
    
    old_parents_path = ' : '.join(old_path.split(' : ')[:-1])
    new_parents_path = ' : '.join(new_path.split(' : ')[:-1])
    old_parent_id = sector['parent_id']
    
    if(old_name != new_name):
        print(f'rename_sector: {old_name} -> {new_name}')
        sector['segment'] = new_path.split(' : ')[-1]
    
    new_parent_id = old_parent_id
    if old_parents_path != new_parents_path:
        if new_parents_path == '':
            new_parent = None
        else:
            new_parent = get_sector(new_parents_path)
            
        if new_parent is not None:
            new_parent_id = new_parent['id']
            sector_cluster_id = new_parent['sector_cluster_id']
        else:
            new_parent_id = None
            
        print(
            f'change_parent: {old_parents_path} ({old_parent_id}) ' \
            f'-> {new_parents_path} ({new_parent_id})'
        )
        sector['parent_id'] = new_parent_id if new_parent is not None else None

    sectors_to_rename_or_adopt.append((old_sector, sector))

@click.command()
@click.option(
    '--sector-mappings-filepath',
    default='sector mappings - all_mappings_combined_2020-06-03.csv'
)
@click.option(
    '--final-sector-list-filepath',
    default='sector mappings - final_sector_list.csv'
)
@click.option(
    '--existing-dh-sectors-filepath',
    default='sector mappings - datahub_metadata_sector.csv'
)
@click.option(
    '--create-sector-ids-filepath',
    default='create_sector_ids.csv'
)
def main(
        sector_mappings_filepath,
        final_sector_list_filepath,
        existing_dh_sectors_filepath,
        create_sector_ids_filepath
):
    print('main')

    global state
    
    df_sector_mappings = load_df_sector_mappings(sector_mappings_filepath)
    df_final_sector_list = load_df_final_sector_list(final_sector_list_filepath)
    df_existing_dh_sectors = load_df_existing_dh_sectors(
        existing_dh_sectors_filepath
    )

    # fixed new sector ids
    df_create_sector_ids = load_df_create_sector_ids(create_sector_ids_filepath)
    new_sectors_ids = {}
    for s in df_create_sector_ids.to_dict(orient='records'):
        new_sectors_ids[s['path']] =  s['id']
    
    new_sectors_cluster_ids = {
        'Space': '531d3510-3f42-41fd-86b5-fa686fdfe33f'
    }

    migrations = df_sector_mappings.to_dict(orient='records')
    migrations = [
        to_migration_object(
            m['old_sector'],
            m['new_sector_fix_capitalisation']
        ) for m in migrations
    ]
    migrations = sorted(
        migrations,
        key=lambda x: (x['new_sector']['level'], x['old_sector']['level'])
    )

    state = {}
    state['migrations'] = migrations
    state['df_create_sector_ids'] = df_create_sector_ids
    state['sectors'] = copy.deepcopy(
        df_existing_dh_sectors.to_dict(orient='records')
    )
    state['sectors_to_create'] = []
    state['sectors_to_delete'] = []
    state['sectors_to_migrate'] = []
    state['sectors_to_rename_or_adopt'] = []
    
    for m in migrations:

        old_sector_path = m['old_sector']['path']
        new_sector_path = m['new_sector']['path']
        print(f'\nmap_sector: {old_sector_path} -> {new_sector_path}')

        if old_sector_path == new_sector_path:
            print('do nothing')
            continue

        if sector_exists(new_sector_path):
            migrate_data(old_sector_path, new_sector_path)
            delete_sector(old_sector_path)
            continue
        else:
            create_parents(
                new_sector_path,
                new_sectors_ids,
                new_sectors_cluster_ids
            )
            update_sector_path(old_sector_path, new_sector_path)
            continue

    # create other new sectors
    for index, row in df_final_sector_list.iterrows():
        path = row['sector']
        if not sector_exists(path):
            id = new_sectors_ids.get(path)
            sector_cluster_id = new_sectors_cluster_ids.get(path)
            create_sector(path, id=id, sector_cluster_id=sector_cluster_id)

    # generate_sectors_to_create_csv
    generate_sectors_to_create_csvs(state['sectors_to_create'])
    generate_sectors_to_rename_or_adopt(state['sectors_to_rename_or_adopt'])
    generate_sectors_to_migrate(state['sectors_to_migrate'])
    generate_sectors_to_delete(state['sectors_to_delete'])

    # check deletions
    print('\nCheck deletions')
    for s in state['sectors_to_delete']:
        sector_id = s['id']
        children = [c for c in state['sectors'] if c['parent_id'] == sector_id]
        children = [c for c in children if c['to_delete'] != True]
        assert len(children) == 0, f'Unable to delete sector {s[1]},' \
            'children will be orphaned: {[c["path"] for c in children]}'
    print('OK')

    # check final state of sectors to final_sector_list
    exceptions = ['More Sectors']
    df_state = pd.DataFrame(state['sectors'])
    df_state = df_state[df_state['to_delete'].isna()]

    # check: sectors that are not in final_sector_list but are in sector state
    df1 = pd.merge(
        df_state,
        df_final_sector_list,
        how='left',
        left_on='path',
        right_on='sector'
    )
    df1 = df1[df1['sector'].isna()]

    # check: sectors that are not in the sector state but are in
    # the final_sector_list
    df2 = pd.merge(
        df_state,
        df_final_sector_list,
        how='right',
        left_on='path',
        right_on='sector'
    )
    df2 = df2[df2['path'].isna()]

    df = pd.concat([df1, df2])
    df = df[~df['path'].isin(exceptions)]
    assert len(df) == 0, f'Mismatch between sector state and ' \
        f'final_sector_list\n{df}'

state = {}
    
if __name__ == '__main__':
    main()
