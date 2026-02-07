package main

import (
	"fmt"
	"log"
	"net"
	"os"

	meridian_api "github.com/c12s/meridian/pkg/api"
	oortapi "github.com/c12s/oort/pkg/api"
	"github.com/jtomic1/config-schema-service/internal/configschema"
	"github.com/jtomic1/config-schema-service/internal/services"
	pb "github.com/jtomic1/config-schema-service/proto"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

func main() {

	shutdownTracing := initTracing(
		"quasar",
		os.Getenv("JAEGER_HOST")+":"+os.Getenv("JAEGER_GRPC_PORT"),
	)
	defer shutdownTracing()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", os.Getenv("SERVER_PORT")))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.UnaryInterceptor(configschema.GetAuthInterceptor()),
	)

	administrator, err := oortapi.NewAdministrationAsyncClient(os.Getenv("NATS_ADDRESS"))
	if err != nil {
		log.Fatalln(err)
	}
	authorizer := services.NewAuthZService(os.Getenv("SECRET_KEY"))
	conn, err := grpc.NewClient(os.Getenv("MERIDIAN_ADDRESS"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalln(err)
	}
	meridian := meridian_api.NewMeridianClient(conn)
	configSchemaServer := configschema.NewServer(authorizer, administrator, meridian)

	pb.RegisterConfigSchemaServiceServer(grpcServer, configSchemaServer)
	reflection.Register(grpcServer)

	log.Printf("Server listening at %v", lis.Addr())
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
