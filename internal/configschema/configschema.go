package configschema

import (
	"context"
	"fmt"
	"log"

	meridian_api "github.com/c12s/meridian/pkg/api"
	oortapi "github.com/c12s/oort/pkg/api"
	"github.com/jtomic1/config-schema-service/internal/repository"
	"github.com/jtomic1/config-schema-service/internal/services"
	"github.com/jtomic1/config-schema-service/internal/validators"
	pb "github.com/jtomic1/config-schema-service/proto"
	"github.com/xeipuuv/gojsonschema"
	"go.opentelemetry.io/otel"
	"golang.org/x/mod/semver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"sigs.k8s.io/yaml"
)

type Server struct {
	pb.UnimplementedConfigSchemaServiceServer
	authorizer    *services.AuthZService
	administrator *oortapi.AdministrationAsyncClient
	meridian      meridian_api.MeridianClient
}

type ConfigSchemaRequest interface {
	GetOrganization() string
	GetSchemaName() string
	GetVersion() string
	GetNamespace() string
}

func NewServer(authorizer *services.AuthZService, administrator *oortapi.AdministrationAsyncClient, meridian meridian_api.MeridianClient) *Server {
	return &Server{
		authorizer:    authorizer,
		administrator: administrator,
		meridian:      meridian,
	}
}

func getConfigSchemaKey(req ConfigSchemaRequest) string {
	return req.GetOrganization() + "/" + req.GetNamespace() + "/" + req.GetSchemaName() + "/" + req.GetVersion()
}

func getConfigSchemaPrefix(req ConfigSchemaRequest) string {
	return req.GetOrganization() + "/" + req.GetNamespace() + "/" + req.GetSchemaName()
}

func (s *Server) SaveConfigSchema(ctx context.Context, in *pb.SaveConfigSchemaRequest) (*pb.SaveConfigSchemaResponse, error) {
	tracer := otel.Tracer("quasar.ConfigSchemaService")
	ctx, span := tracer.Start(ctx, "ConfigSchemaService.SaveConfigSchema")
	defer span.End()

	ctx = s.authorizer.SetOutgoingContext(ctx)
	_, err := s.meridian.GetNamespace(ctx, &meridian_api.GetNamespaceReq{
		OrgId: in.SchemaDetails.Organization,
		Name:  in.SchemaDetails.Namespace,
	})
	if err != nil {
		return nil, err
	}
	if !s.authorizer.Authorize(ctx, services.PermSchemaPut, services.OortResNamespace, fmt.Sprintf("%s/%s", in.SchemaDetails.Organization, in.SchemaDetails.Namespace)) {
		return nil, fmt.Errorf("permission denied: %s", services.PermSchemaPut)
	}
	_, err = validators.IsSaveSchemaRequestValid(in)
	if err != nil {
		return &pb.SaveConfigSchemaResponse{
			Status:  3,
			Message: err.Error(),
		}, nil
	}
	repoClient, err := repository.NewClient()
	if err != nil {
		return &pb.SaveConfigSchemaResponse{
			Status:  13,
			Message: "Error while instantiating database client!",
		}, nil
	}
	defer repoClient.Close()

	latestVersion, err := repoClient.GetLatestVersionByPrefix(ctx, getConfigSchemaPrefix(in.GetSchemaDetails()))
	if err != nil {
		return &pb.SaveConfigSchemaResponse{
			Status:  13,
			Message: err.Error(),
		}, nil
	}
	if latestVersion != "" && semver.Compare(in.GetSchemaDetails().GetVersion(), latestVersion) != 1 {
		return &pb.SaveConfigSchemaResponse{
			Status:  3,
			Message: "Provided version is not latest! Please provide a version that succeeds '" + latestVersion + "'!",
		}, nil
	}
	err = repoClient.SaveConfigSchema(ctx, getConfigSchemaKey(in.GetSchemaDetails()), in.GetSchema())
	if err != nil {
		return &pb.SaveConfigSchemaResponse{
			Status:  13,
			Message: err.Error(),
		}, nil
	}
	err = s.administrator.SendRequest(&oortapi.CreateInheritanceRelReq{
		From: &oortapi.Resource{
			Id:   in.SchemaDetails.Organization,
			Kind: services.OortResOrg,
		},
		To: &oortapi.Resource{
			Id:   services.OortSchemaId(in.SchemaDetails.Organization, in.SchemaDetails.Namespace, in.SchemaDetails.SchemaName, in.SchemaDetails.Version),
			Kind: services.OortResSchema,
		},
	}, func(resp *oortapi.AdministrationAsyncResp) {
		log.Println(resp.Error)
	})
	if err != nil {
		log.Println(err)
	}
	return &pb.SaveConfigSchemaResponse{
		Status:  0,
		Message: "Schema saved successfully!",
	}, nil
}

func (s *Server) GetConfigSchema(ctx context.Context, in *pb.GetConfigSchemaRequest) (*pb.GetConfigSchemaResponse, error) {
	tracer := otel.Tracer("quasar.ConfigSchemaService")
	ctx, span := tracer.Start(ctx, "ConfigSchemaService.GetConfigSchema")
	defer span.End()

	oortSchemaId := services.OortSchemaId(in.SchemaDetails.Organization, in.SchemaDetails.Namespace, in.SchemaDetails.SchemaName, in.SchemaDetails.Version)
	if !s.authorizer.Authorize(ctx, services.PermSchemaGet, services.OortResSchema, oortSchemaId) {
		return nil, fmt.Errorf("permission denied: %s", services.PermSchemaGet)
	}
	_, err := validators.IsGetSchemaRequestValid(in)
	if err != nil {
		return &pb.GetConfigSchemaResponse{
			Status:     3,
			Message:    err.Error(),
			SchemaData: nil,
		}, nil
	}
	repoClient, err := repository.NewClient()
	if err != nil {
		return &pb.GetConfigSchemaResponse{
			Status:     13,
			Message:    "Error while instantiating database client!",
			SchemaData: nil,
		}, nil
	}
	defer repoClient.Close()

	key := getConfigSchemaKey(in.GetSchemaDetails())
	schemaData, err := repoClient.GetConfigSchema(ctx, key)
	if err != nil {
		return &pb.GetConfigSchemaResponse{
			Status:     13,
			Message:    "Error while retrieving schema!",
			SchemaData: nil,
		}, nil
	}
	var message string
	if schemaData == nil {
		message = "No schema with key '" + key + "' found!"
	} else {
		message = "Schema retrieved successfully!"
	}
	return &pb.GetConfigSchemaResponse{
		Status:     0,
		Message:    message,
		SchemaData: schemaData,
	}, nil
}

func (s *Server) DeleteConfigSchema(ctx context.Context, in *pb.DeleteConfigSchemaRequest) (*pb.DeleteConfigSchemaResponse, error) {
	tracer := otel.Tracer("quasar.ConfigSchemaService")
	ctx, span := tracer.Start(ctx, "ConfigSchemaService.DeleteConfigSchema")
	defer span.End()

	oortSchemaId := services.OortSchemaId(in.SchemaDetails.Organization, in.SchemaDetails.Namespace, in.SchemaDetails.SchemaName, in.SchemaDetails.Version)
	if !s.authorizer.Authorize(ctx, services.PermSchemaDel, services.OortResSchema, oortSchemaId) {
		return nil, fmt.Errorf("permission denied: %s", services.PermSchemaDel)
	}
	_, err := validators.IsDeleteSchemaRequestValid(in)
	if err != nil {
		return &pb.DeleteConfigSchemaResponse{
			Status:  3,
			Message: err.Error(),
		}, nil
	}
	repoClient, err := repository.NewClient()
	if err != nil {
		return &pb.DeleteConfigSchemaResponse{
			Status:  13,
			Message: "Error while instantiating database client!",
		}, nil
	}
	defer repoClient.Close()

	if err := repoClient.DeleteConfigSchema(ctx, getConfigSchemaKey(in.GetSchemaDetails())); err != nil {
		return &pb.DeleteConfigSchemaResponse{
			Status:  3,
			Message: err.Error(),
		}, nil
	} else {
		return &pb.DeleteConfigSchemaResponse{
			Status:  0,
			Message: "Schema deleted successfully!",
		}, nil
	}
}

func (s *Server) ValidateConfiguration(ctx context.Context, in *pb.ValidateConfigurationRequest) (*pb.ValidateConfigurationResponse, error) {
	tracer := otel.Tracer("quasar.ConfigSchemaService")
	ctx, span := tracer.Start(ctx, "ConfigSchemaService.ValidateConfiguration")
	defer span.End()

	oortSchemaId := services.OortSchemaId(in.SchemaDetails.Organization, in.SchemaDetails.Namespace, in.SchemaDetails.SchemaName, in.SchemaDetails.Version)
	if !s.authorizer.Authorize(ctx, services.PermSchemaGet, services.OortResSchema, oortSchemaId) {
		return nil, fmt.Errorf("permission denied: %s", services.PermSchemaGet)
	}
	isValid, err := validators.IsValidateConfigurationRequestValid(in)
	if err != nil {
		return &pb.ValidateConfigurationResponse{
			Status:  3,
			Message: err.Error(),
			IsValid: isValid,
		}, nil
	}
	repoClient, err := repository.NewClient()
	if err != nil {
		return &pb.ValidateConfigurationResponse{
			Status:  13,
			Message: "Error while instantiating database client!",
			IsValid: false,
		}, nil
	}
	defer repoClient.Close()

	key := getConfigSchemaKey(in.GetSchemaDetails())
	schemaData, err := repoClient.GetConfigSchema(ctx, key)
	if err != nil {
		return &pb.ValidateConfigurationResponse{
			Status:  13,
			Message: "Error while instantiating database client!",
			IsValid: false,
		}, nil
	} else if schemaData == nil {
		return &pb.ValidateConfigurationResponse{
			Status:  3,
			Message: "No schema with key '" + key + "' found!",
			IsValid: false,
		}, nil
	}
	validationResult, err := validateConfiguration(in.GetConfiguration(), schemaData.GetSchema())
	if err != nil {
		return &pb.ValidateConfigurationResponse{
			Status:  3,
			Message: "Error while validating schema!",
			IsValid: false,
		}, nil
	}
	var message string
	if validationResult.Valid() && message == "" {
		message = "The configuration is valid!"
	} else {
		message = validationResult.Errors()[0].String()
	}

	return &pb.ValidateConfigurationResponse{
		Status:  0,
		Message: message,
		IsValid: validationResult.Valid(),
	}, nil
}

func validateConfiguration(configuration string, schema string) (*gojsonschema.Result, error) {
	configurationJson, err := yaml.YAMLToJSON([]byte(configuration))
	if err != nil {
		return nil, err
	}
	schemaJson, err := yaml.YAMLToJSON([]byte(schema))
	if err != nil {
		return nil, err
	}
	configLoader := gojsonschema.NewStringLoader(string(configurationJson))
	schemaLoader := gojsonschema.NewStringLoader(string(schemaJson))
	result, err := gojsonschema.Validate(schemaLoader, configLoader)
	if err != nil {
		return nil, err
	} else {
		return result, nil
	}
}

func (s *Server) GetConfigSchemaVersions(ctx context.Context, in *pb.ConfigSchemaVersionsRequest) (*pb.ConfigSchemaVersionsResponse, error) {
	tracer := otel.Tracer("quasar.ConfigSchemaService")
	ctx, span := tracer.Start(ctx, "ConfigSchemaService.GetConfigSchemaVersions")
	defer span.End()

	if !s.authorizer.Authorize(ctx, services.PermSchemaPut, services.OortResNamespace, fmt.Sprintf("%s/%s", in.SchemaDetails.Organization, in.SchemaDetails.Namespace)) {
		return nil, fmt.Errorf("permission denied: %s", services.PermSchemaGet)
	}
	_, err := validators.IsGetConfigSchemaVersionsValid(in)
	if err != nil {
		return &pb.ConfigSchemaVersionsResponse{
			Status:  3,
			Message: err.Error(),
		}, nil
	}
	repoClient, err := repository.NewClient()
	if err != nil {
		return &pb.ConfigSchemaVersionsResponse{
			Status:  13,
			Message: "Error while instantiating database client!",
		}, nil
	}
	defer repoClient.Close()

	key := getConfigSchemaPrefix(in.GetSchemaDetails())
	schemaVersions, err := repoClient.GetSchemasByPrefix(ctx, key)
	if err != nil {
		return &pb.ConfigSchemaVersionsResponse{
			Status:  13,
			Message: "Error while retrieving schema!",
		}, nil
	}
	var message string
	if schemaVersions == nil {
		message = "No schema with prefix '" + key + "' found!"
	} else {
		message = "Schema versions retrieved successfully!"
	}
	return &pb.ConfigSchemaVersionsResponse{
		Status:         0,
		Message:        message,
		SchemaVersions: schemaVersions,
	}, nil
}

func GetAuthInterceptor() func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if ok && len(md.Get("authz-token")) > 0 {
			ctx = context.WithValue(ctx, "authz-token", md.Get("authz-token")[0])
		}
		return handler(ctx, req)
	}
}
