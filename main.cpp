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

std::vector<Paquete> buffer1;//estanteria
std::queue<Paquete> buffer2;//cinta

std::mutex mtxBuffer1;
std::mutex mtxBuffer2;
std::mutex mtxConsola;
std::mutex mtxPedido;
std::mutex mtxPendientes;
std::mutex mtxId;

int pedido = 30;
int nivelPrioridad = 5;

int paquetesPendientes = pedido;
int siguienteId = 1;
int contadorAltas = 0;
int marcador = 0;

Semaforo hayDatosBuffer1; //waitingQueue
Semaforo hayDatosBuffer2; //Processing Queue
Semaforo HayEspacioBuffer2; //Processing Queue


void productor() //Guardamos los paquetes en el buffer 1
{
    while(pedido>0)
    {
        mtxPedido.lock();
    if(pedido > 0){
        pedido--;
        mtxPedido.unlock();
    } else {
        mtxPedido.unlock();
        break;
    }

        Paquete p;

        mtxId.lock();
        p.id = siguienteId++;
        mtxId.unlock();

        p.prioridad = rand() % 2;
        p.fechaDeCreacion = chrono::system_clock::now(); //Guardamos la fecha de hoy

        //wait(HayEspacioBuffer1);

        mtxBuffer1.lock();

        if(p.prioridad == 1){

            buffer1.insert(buffer1.begin() + marcador, p);

            marcador++;
        }else{

            buffer1.push_back(p);
        }

        mtxConsola.lock();
        std::cout << "paquete creado: ";
        std::cout << "ID: " << p.id << " Prioridad: " << p.prioridad << std::endl;
        mtxConsola.unlock();

        mtxBuffer1.unlock();


        signal(hayDatosBuffer1);

        this_thread::sleep_for(chrono::milliseconds(90));
    }
}

void consumidor() //Saca los paquetes del buffer 2
{
    while(paquetesPendientes > 0)
    {
        wait(hayDatosBuffer2);

        //control de pendientes
        mtxPendientes.lock();
        if(paquetesPendientes > 0){
            paquetesPendientes--;
            mtxPendientes.unlock();
        } else {
            mtxPendientes.unlock();
            break;
        }

        Paquete p;

        mtxBuffer2.lock();
        p = buffer2.front(); //primer elementos
        buffer2.pop();
        mtxBuffer2.unlock();

        signal(HayEspacioBuffer2);

        mtxConsola.lock();

        time_t t = chrono::system_clock::to_time_t(p.fechaDeCreacion);

        cout << "ID: " << p.id << " Prioridad: " << p.prioridad << " Fecha: " << ctime(&t); // lo pasa a un texto legible

        mtxConsola.unlock();
    }

}

void cargarEnCinta()
{
    while(paquetesPendientes > 0)
    {
        paquetesPendientes--;

        wait(hayDatosBuffer1);
        wait(HayEspacioBuffer2);

        std::this_thread::sleep_for(std::chrono::milliseconds(420));

        Paquete p;

        mtxBuffer1.lock();

        if(contadorAltas < nivelPrioridad && marcador > 0)
        {
            p = buffer1.front();
            buffer1.erase(buffer1.begin());
            marcador--;
            contadorAltas++;
        }
        else if(marcador < buffer1.size())
        {
            p = buffer1[marcador];
            buffer1.erase(buffer1.begin() + marcador);
            contadorAltas = 0;
        }
        else if(marcador > 0)
        {
            p = buffer1.front();
            buffer1.erase(buffer1.begin());
            marcador--;
            contadorAltas++;
        }

        mtxBuffer1.unlock();

        mtxBuffer2.lock();
        buffer2.push(p);
        mtxBuffer2.unlock();

        mtxConsola.lock();
        std::cout << "paquete en cinta " << p.id << " prioridad " << p.prioridad << std::endl;
        mtxConsola.unlock();

        signal(hayDatosBuffer2);
    }
}


int main()
{
    init(HayEspacioBuffer2,5);
    init(hayDatosBuffer1,0);
    init(hayDatosBuffer2,0);

    thread p1(productor);
    thread c1(cargarEnCinta);
    thread c2(cargarEnCinta);
    thread r1(consumidor);

    p1.join();
    c1.join();
    c2.join();
    r1.join();


}
//mutex en zonas criticas
//agregar funcion cargarEnCinta a la funcion Productor
//añadir retardos
//ejecutar los escenarios de pruebas

