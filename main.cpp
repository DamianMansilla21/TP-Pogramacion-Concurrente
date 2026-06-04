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
    std::chrono::system_clock::time_point fechaDeCreacion;
};

std::queue<Paquete> buffer1;//estanteria
std::queue<Paquete> buffer2;//cinta

Semaforo espacioEnCinta;
Semaforo paquetesEnEstanteria;
Semaforo paquetesEnCinta;

std::mutex mtxBufer1;
std::mutex mtxBufer2;
std::mutex mtxConsola;

int pedido = 5;
int siguienteId = 1;

Semaforo hayDatosBuffer1; //waitingQueue
Semaforo HayEspacioBuffer1; //waitingQueue
Semaforo hayDatosBuffer2; //Processing Queue
Semaforo HayEspacioBuffer2; //Processing Queue


void productor(int cantidad) //Guardamos los paquetes en el buffer 1
{
    for (int i = 0; i < cantidad; i++)
    {
        Paquete p;

        p.id = siguienteId++;
        p.prioridad = rand() % 2;
        p.fechaDeCreacion = chrono::system_clock::now(); //Guardamos la fecha de hoy
        
        wait(HayEspacioBuffer1);
        mtx_bufer1.lock();
        buffer1.push(p);
        mtx_bufer1.unlock();

        signal(hayDatosBuffer1);

        this_thread::sleep_for(chrono::milliseconds(90));
    }
}

void consumidor(int cant) //Saca los paquetes del buffer 2
{
    for (int x = 0; x<cant; x++)
    {
        wait(hayDatosBuffer2);

        Paquete p;

        mtx_bufer2.lock();
        p = buffer2.front(); //primer elementos
        buffer2.pop();
        mtx_bufer2.unlock();

        signal(HayEspacioBuffer2);

        mtx_salida.lock();

        time_t t = chrono::system_clock::to_time_t(p.fechaDeCreacion);

        cout << "ID: " << p.id << " Prioridad: " << p.prioridad << " Fecha: " << ctime(&t); // lo pasa a un texto legible

        mtx_salida.unlock();
    }
}

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
