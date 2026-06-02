#include <iostream>
#include <thread>
#include <mutex>
#include <chrono>
#include <queue>

#include "semaforo.h"

using namespace std;

struct Paquete {
    int id;
    int prioridad;
    std::string fechaCreacion;
};

std::queue<Paquete> bufer1;//estanteria
std::queue<Paquete> bufer2;//cinta

Semaforo espacioEnCinta;
Semaforo paquetesEnEstanteria;
Semaforo paquetesEnCinta;

std::mutex mtxBufer1;
std::mutex mtxBufer2;
std::mutex mtxConsola;

int pedido = 5;
int siguienteId = 1;

void generarPaquete(){

    while (pedido > 0){

        Paquete p;
        p.id = siguienteId;
        p.prioridad = 0;
        p.fechaCreacion = "10/01/2026";

        bufer1.push(p);
        pedido --;
        mtxConsola.lock();
        std::cout << "paquete en estanteria " << p.id << endl;
        mtxConsola.unlock();
        signal(paquetesEnEstanteria);

        siguienteId++;
    }
}

void cargarEnCinta(){

    while(true){

        wait(paquetesEnEstanteria);
        wait(espacioEnCinta);

        std::this_thread::sleep_for(std::chrono::milliseconds(420));

        Paquete p = bufer1.front();
        bufer1.pop();

        bufer2.push(p);

        mtxConsola.lock();
        std::cout << "paquete en cinta " << p.id << endl;
        mtxConsola.unlock();

        signal(paquetesEnCinta);
    }

}

void procesarPaquetes(){

    while(true){

        wait(paquetesEnCinta);

        std::this_thread::sleep_for(std::chrono::milliseconds(270));

        Paquete p = bufer2.front();
        bufer2.pop();

        mtxConsola.lock();
        std::cout << "paquete procesado " << p.id << endl;
        mtxConsola.unlock();

        signal(espacioEnCinta);
    }
}

int main()
{
    init(espacioEnCinta,5);
    init(paquetesEnEstanteria,0);
    init(paquetesEnCinta,0);

    thread p1(generarPaquete);
    thread c1(cargarEnCinta);
    thread r1(procesarPaquetes);

    p1.join();
    c1.join();
    r1.join();


}

//prioridad aleatoria
//fecha de creacion
//ubicar lock y unlock
//unir crearPaquete y cargarEnCintan en una sola funcion
//retardos simulados
