import yaml
import os
import hashlib
import zlib
import math
import re
import multiprocessing
import threading

class read_change_lock:
    def __init__(self):
        self._lock = threading.Lock()
        self._read_condition = threading.Condition(self._lock)
        self._change_condition = threading.Condition(self._lock)
        self._readers = 0
        self._changers = 0

    def acquire_read(self):
        with self._lock:
            while self._changers > 0:
                self._read_condition.wait()
            self._readers += 1
    
    def release_read(self):
        with self._lock:
            self._readers -= 1
            if self._readers == 0:
                self._change_condition.notify()
    
    def acquire_change(self):
        with self._lock:
            while self._readers > 0 or self._changers > 0:
                self._change_condition.wait()
            self._changers += 1
    
    def release_change(self):
        with self._lock:
            self._changers -= 1
            # Ovde ne treba notify_all(), jer samo ce uvek self._lock biti za jednog acquire-ovan
            self._read_condition.notify()
            self._change_condition.notify()


class my_file:
    def __init__(self, name, id, status, number_of_parts):
        self.name = name
        self.id = id
        self.copies_counter = 0
        self.status = status
        self.number_of_parts = number_of_parts
    def __str__(self):
        return f'Name: {self.name}, ID: {self.id}, Status: {self.status}, Number of parts: {self.number_of_parts}'

class part_of_file:
    def __init__(self, part_name, part_id, my_file_id, index, md5_of_part):
        self.part_name = part_name
        self.part_id = part_id
        self.my_file_id = my_file_id
        self.index = index
        self.md5_of_part = md5_of_part
    def __str__(self):
        return f'Name: {self.part_name}, ID: {self.part_id}, MY_FILE_ID: {self.my_file_id}, Index: {self.index}, MD5: {self.md5_of_part}'

path_of_parts = ""
number_of_processes = 0
memory_limit_in_bytes = 0
PART_BYTES = 10
PART_SAMPLE_SIZE = 10

id_counter = 0
part_id_counter = 0
v_my_file = []
v_part_of_file = []

put_names_queue = []

current_memory_in_use = 0


pool = None
my_file_lock = None
part_of_file_lock = None
id_counter_lock = threading.Lock()
part_id_counter_lock = threading.Lock()
put_names_queue_lock = threading.Lock()
memory_change_lock = threading.Lock()
memory_lock_for_main = threading.Condition()
out_lock = threading.Lock()

def initialize_constants(dict):
    global path_of_parts, number_of_processes, memory_limit_in_bytes
    path_of_parts = os.getcwd() + dict.get("path")
    number_of_processes = dict.get("process_num")
    memory_limit_in_bytes = dict.get("memory_limit")
    return

def create_my_file_instance(file_name, counter_of_same_names_in_queue):
    global id_counter, v_my_file, my_file_lock, id_counter_lock
    my_file_size = os.path.getsize(file_name)
    number_of_parts = int(math.ceil(my_file_size / PART_BYTES))
    # Kriticna sekcija za pristup i menjanje
    id_counter_lock.acquire()
    id = id_counter
    id_counter += 1
    id_counter_lock.release()
    status = False    
    extracted_file_name = file_name.split(".txt")[0]
    pattern_name = rf"({extracted_file_name}\([1-9][0-9]*\)|{extracted_file_name}).txt"
    counter = 0
    # Kriticna sekcija za citanje
    my_file_lock.acquire_read()
    for x in v_my_file:
        # print("Usao sam")
        if re.match(pattern_name, x.name) is not None:
            # print("Regex radi super ddx")
            counter += 1
    my_file_lock.release_read()    
    counter += counter_of_same_names_in_queue # ovo ce skoro uvek biti 0 ali je opet bitan bug fix
    # print("Na kraju prolaska kroz niz counter je: " + str(counter))
    name = ""
    if counter == 0:
        name = f'{extracted_file_name}.txt'
    else:
        name = f'{extracted_file_name}({counter}).txt'
    file_instance = my_file(name, id, status, number_of_parts)
    # Ne treba ovde jos append, jer moramo promeniti status na kraju put funkcije, tako da je samo prirodno da tamo uradimo append
    # v_my_file.append(file_instance)
    # print(file_instance)
    return file_instance

def create_part_of_file(content, file_instance_name, i, path_of_parts):
    first_digest = hashlib.md5(content).hexdigest()
    compressed = zlib.compress(content)
    new_compressed_path_name = os.path.join(path_of_parts, f'{file_instance_name.split(".txt")[0]}_{i}.dat')
     
    with open(new_compressed_path_name, "wb") as compressed_part_file:
        compressed_part_file.write(compressed)
    return first_digest

def put_command(file_name):
    global v_my_file, v_part_of_file, part_id_counter, path_of_parts, part_of_file_lock, my_file_lock, put_names_queue, put_names_queue_lock, current_memory_in_use, memory_lock_for_main, memory_limit_in_bytes, memory_change_lock
    # Ubacujem u queue zato sto ovde se moze desiti da dva put poziva su za dva file-a koji su isti
    counter_of_same_names_in_queue = 0
    put_names_queue_lock.acquire()
    for x in put_names_queue:
        if x == file_name:
            counter_of_same_names_in_queue += 1
    put_names_queue.append(file_name)
    put_names_queue_lock.release()
    
    # ISPRAVKA PROCESA
    file_instance = create_my_file_instance(file_name, counter_of_same_names_in_queue)

    # Na kraju ove komande cemo promeniti status na True
    # Sada pravimo delove za prethodno napravljen my_file instance
    file_path = os.getcwd() + "\\" + file_name
    with open(file_path, "rb") as file:
        i = 1
        my_file_size = os.path.getsize(file_path)
        number_of_parts = int(math.ceil(my_file_size / PART_BYTES))
        while True:
            # Okej ovde znaci sve sto treba da malo izmenim je da dodam jedan for loop u kome pravim const = 10 procesa koji svaki obradjuje jedan deo
            # pa nakon toga kad te procese upamtim da onda nateram da saceka trenutni thread na svaki od njih i onda kad se svi zavrse opet moze da thread radi dalje
            # Voditi racuna o tome da ostatak nekog file moze biti manji od 10 * velicina_dela!!!
            if number_of_parts < i:
                break
            iterations = number_of_parts - i + 1 if number_of_parts - i + 1 < PART_SAMPLE_SIZE else PART_SAMPLE_SIZE
            with memory_lock_for_main:
                while current_memory_in_use + iterations * PART_BYTES > memory_limit_in_bytes:
                    memory_lock_for_main.wait()
                memory_change_lock.acquire()
                current_memory_in_use += iterations * PART_BYTES
                memory_change_lock.release()
                memory_lock_for_main.notify_all()
            
            # Sad ide dobra for petlja                      
            v_processes = []
            for j in range(0, iterations, 1):
                content = file.read(PART_BYTES)
                # Necu odmah da pravim part_id nego kasnije kada sve budem obradjivao
                new_part_creation_process = pool.apply_async(create_part_of_file, (content, file_instance.name, i, path_of_parts))
                v_processes.append((i, new_part_creation_process))
                i += 1

            # Sada teram thread da ceka da se svi delovi zavrse pre nego sto dalje nastavi da radi
            for x in v_processes:
                part_id_counter_lock.acquire()
                part_id = part_id_counter
                part_id_counter += 1
                part_id_counter_lock.release()
                first_digest = x[1].get()
                part_of_file_instance = part_of_file(f'{file_instance.name.split(".txt")[0]}_{x[0]}.dat', part_id, file_instance.id, x[0], first_digest)
                part_of_file_lock.acquire_change()
                v_part_of_file.append(part_of_file_instance)
                part_of_file_lock.release_change()
                memory_change_lock.acquire()
                current_memory_in_use -= PART_BYTES
                memory_change_lock.release()
                     
    # Sada kada cu ubaciti file_instance treba da iz queue-a izbacim njegovo ime
    put_names_queue_lock.acquire()
    for x in put_names_queue:
        if x == file_name:
            put_names_queue.remove(x)
            break

    file_instance.status = True
    my_file_lock.acquire_change()
    v_my_file.append(file_instance)
    my_file_lock.release_change()
    # Release je skroz ovde zato sto necemo da dozvolimo da prodje neki put poziv dalje i da onda je stigao pre nego sto se dodao file u v_my_file, jer smo obrisali iz queue-a
    put_names_queue_lock.release()
    
    return

def create_new_file_from_paths(part_path, first_digest):
    # Sad moram da otvorim onaj .dat file da bih uzeo zapravo podatke
    compressed = b""
    res = b""
    with open(part_path, "rb") as compressed_part_file:
        compressed = compressed_part_file.read()
    try:
        decompressed = zlib.decompress(compressed)
    except zlib.error as e:
        return None
    second_digest = hashlib.md5(decompressed).hexdigest()

    '''
    if first_digest != second_digest:
        return None
    '''
    res += decompressed
    return res

def get_command(file_id):
    global v_my_file, v_part_of_file, my_file_lock, part_of_file_lock, path_of_parts, current_memory_in_use, memory_change_lock, memory_limit_in_bytes, memory_lock_for_main, out_lock
    # Sad moram nekako da uzmem sve elemente iz niza v_part_of_file i da ih za svaki slucaj sortiram po polju index i da onda za taj novi niz uradim decompress u novi file
    # Odnosno ajde za pocetak, posto unosim redom i lock-ujem niz, da zapravo ne radim sortiranje, jer proces ima samo jedan thread koji radi sto znaci da ce biti dobar redosled
    my_file_scavenged = None
    v_wanted_parts = []    
    my_file_lock.acquire_read()
    for x in v_my_file:
        if x.id == file_id:
            my_file_scavenged = x
            break
    my_file_lock.release_read()
    if my_file_scavenged is None:
        with out_lock:
            print("Ne postoji file koji ima uneti id!")
        return
    part_of_file_lock.acquire_read()
    for x in v_part_of_file:
        if x.my_file_id == file_id:
            v_wanted_parts.append(x)
    part_of_file_lock.release_read()        
    
    # Zapravo mi samo ni ne treba da sortiram zato sto ce vec biti pravilan raspored, jer gore ubacujem pravilnim redosledom
    extracted_file_name = my_file_scavenged.name.split(".txt")[0]
    new_file_path = os.path.join(os.getcwd(), f'processed_{extracted_file_name}.txt')
    with open(new_file_path, "w") as file:
        pass
    i = 0
    while i < my_file_scavenged.number_of_parts:
        iterations = my_file_scavenged.number_of_parts - i if my_file_scavenged.number_of_parts - i < PART_SAMPLE_SIZE else PART_SAMPLE_SIZE
        with memory_lock_for_main:
            while current_memory_in_use + iterations * PART_BYTES > memory_limit_in_bytes:
                memory_lock_for_main.wait()
            memory_change_lock.acquire()
            current_memory_in_use += iterations * PART_BYTES
            memory_change_lock.release()
            memory_lock_for_main.notify_all()

        k = iterations
        v_processes = []
        while k > 0:
            part_path = path_of_parts + "\\" + v_wanted_parts[i].part_name
            new_physical_file_creation_process = pool.apply_async(create_new_file_from_paths, (part_path, v_wanted_parts[i].md5_of_part))
            v_processes.append(new_physical_file_creation_process)
            i += 1
            k -= 1
        
        corrupt_truth = False
        for x in v_processes:
            content = x.get()
            if not content:
                # Ovde ne smem out_lock zato sto nakon ovoga idalje program radi, a za sve ostale slucajeve ispisivanja je nakon print-a odmah return
                # Smem ukoliko input() nije pod out_lock                
                print("Neki deo je corrupt")
                corrupt_truth = True
            if not corrupt_truth:
                with open(new_file_path, "ab") as file:
                    file.write(content)
            memory_change_lock.acquire()
            current_memory_in_use -= PART_BYTES
            memory_change_lock.release()            
        # Kada zavrsim ovo pitam ukoliko je doslo do greske i brisem delove i sam nov file
        if corrupt_truth:
            delete_command(my_file_scavenged.id)
            os.remove(new_file_path)
            return
    
    with out_lock:
        print("Uspesno smo napravili file bez ikakvog corrupt-a")
    return

def delete_physical_part(part_of_file_scavenged, path_of_parts):    
    absolute_path = path_of_parts + "\\" + part_of_file_scavenged.part_name
    os.remove(absolute_path)
    return

def delete_command(file_id):
    global v_my_file, v_part_of_file, path_of_parts, my_file_lock, part_of_file_lock, out_lock
    my_file_lock.acquire_change()
    truth = False
    for x in v_my_file:
        if x.id == file_id:
            x.status = False
            truth = True
            break    
    if not truth:
        with out_lock:
            print("Ne postoji file sa unetom vrednosti id-a")
        return
    my_file_lock.release_change()
    # Sad brisem delove za specificni file iz niza struktura part_of_file i fizicki .dat file koji correspond-uje trenutno posmatranoj strukturi u for-u
    part_of_file_lock.acquire_change()
    # print(f'Duzina part niza: {len(v_part_of_file)}')
    for i in range(len(v_part_of_file) - 1, -1, -1):
        if v_part_of_file[i].my_file_id == file_id:
            # Ovde pravimo async
            # delete_physical_part(v_part_of_file[i])
            new_physical_part_deletion_process = pool.apply_async(delete_physical_part, (v_part_of_file[i], path_of_parts))
            # Ne verujem da ovde treba ikakav memory condition
            del v_part_of_file[i]
    part_of_file_lock.release_change()
    # Sad brisem sam file iz niza struktura my_file
    my_file_lock.acquire_change()
    # print(f'Duzina niza: {len(v_my_file)}')
    for x in v_my_file:
        if x.id == file_id:
            v_my_file.remove(x)
            break
    my_file_lock.release_change()
    # print(f'Vrv malo manja duzina niza xd: {len(v_my_file)}')
    # print(f'Vrv malo manja duzina part niza xd: {len(v_part_of_file)}')
    return

def list_command():
    global v_my_file, v_part_of_file, my_file_lock, part_of_file_lock
    my_file_lock.acquire_read()
    with out_lock:
        for x in v_my_file:
            print(f'File koji se zove {x.name} ima sledece delove u sistemu:')
            part_of_file_lock.acquire_read()
            for y in v_part_of_file:
                if y.my_file_id == x.id:
                    print(y.part_name)
            part_of_file_lock.release_read()
    my_file_lock.release_read()
    return

def main():
    global pool, my_file_lock, part_of_file_lock, number_of_processes, current_memory_in_use, memory_lock_for_main, memory_limit_in_bytes, out_lock
    rez = yaml.safe_load(open(os.getcwd() + "\config.yaml"))
    initialize_constants(rez)
    
    my_file_lock = read_change_lock()
    part_of_file_lock = read_change_lock()

    pool = multiprocessing.Pool(number_of_processes)

    threads = []

    while(True):
        # Neka helper funkcija ce naterati main funkciju da saceka, verujem da je ovo u redu condition za main funkciju
        with memory_lock_for_main:
            # Moze da se azurira i ovde, ali i da se ostavi samo PART_BYTES je idalje relativno validno zato sto neki proces mozda bude radio sa manje od 10 delova
            while current_memory_in_use + PART_BYTES > memory_limit_in_bytes:
                memory_lock_for_main.wait()
                
        with out_lock:
            command = input()
        command_info = command.split(" ")
        if(command_info[0] == "put" and len(command_info) == 2):
            file_name = command_info[1]
            # put_command(file_name)
            thread_put = threading.Thread(target = put_command, args = (file_name,))
            thread_put.start()
            threads.append(thread_put)
            
        elif(command_info[0] == "get" and len(command_info) == 2):
            file_id = int(command_info[1])
            # get_command(file_id)
            thread_get = threading.Thread(target = get_command, args = (file_id,))
            thread_get.start()
            threads.append(thread_get)
            
        elif(command_info[0] == "delete" and len(command_info) == 2):
            file_id = int(command_info[1])
            # delete_command(file_id)
            thread_delete = threading.Thread(target = delete_command, args = (file_id,))
            thread_delete.start()
            threads.append(thread_delete)
            
        elif(command_info[0] == "list" and len(command_info) == 1):
            # list_command()
            thread_list = threading.Thread(target = list_command)
            thread_list.start()
            # threads.append(thread_list)
            # Ova linija dole je verovatno nepotrebna, ali moze da se stavi ukoliko zelimo da main process saceka da se ispise na konzolu
            thread_list.join()
            
        elif(command_info[0] == "exit" and len(command_info) == 1):
            with out_lock:
                print("Izaci cemo iz programa cim se niti i procesi uradno ugase...")
            for thread in threads:
                if thread.is_alive():
                    thread.join()
            # print("Izlazimo iz programa...")
            break
        else:
            with out_lock:
                print("The command you entered is not valid, try again xd")
    print("Gotov program!")
    return

if __name__ == '__main__':
    main()
