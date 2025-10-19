package main

import (
	"context"
	"fmt"
	"log"

	proto "example1/proto"

	"google.golang.org/grpc"
)

func main() {
	// Conectar al broker
	conn, err := grpc.Dial("broken:54000", grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("Error al conectar con el broker: %v", err)
	}
	defer conn.Close()

	// Crear el cliente gRPC
	client := proto.NewBrokerServiceClient(conn)

	// Aquí, el consumidor solicita su historial de ofertas (por ejemplo, usando su ID)
	consumerID := "C-E1" // Este es el ID de consumidor que se define en el archivo consumidores.csv

	// Solicitar el historial de ofertas
	req := &proto.HistoryRequest{
		ConsumidorId: consumerID,
	}
	resp, err := client.GetHistory(context.Background(), req)
	if err != nil {
		log.Fatalf("Error al obtener el historial de ofertas: %v", err)
	}

	// Mostrar las ofertas recibidas
	fmt.Println("Ofertas recibidas:")
	for _, offer := range resp.Offers {
		fmt.Printf("Oferta ID: %s, Producto: %s, Precio: %.2f\n", offer.OfertaId, offer.Producto, offer.Precio)
	}
}
