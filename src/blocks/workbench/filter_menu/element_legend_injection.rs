use grapher::prelude::{
    ElementType, GenericEdge, GenericNode, GenericType, OwlEdge, OwlNode, OwlType, RdfEdge,
    RdfNode, RdfType, RdfsEdge, RdfsNode, RdfsType, XSDNode, XSDType,
};

pub trait ElementLegend {
    /// Get the legend of `self`.
    fn legend(self) -> Option<String>;
}

impl ElementLegend for ElementType {
    fn legend(self) -> Option<String> {
        match self {
            Self::NoDraw => None,
            Self::Rdf(RdfType::Node(node)) => node.legend(),
            Self::Rdf(RdfType::Edge(edge)) => edge.legend(),
            Self::Rdfs(RdfsType::Node(node)) => node.legend(),
            Self::Rdfs(RdfsType::Edge(edge)) => edge.legend(),
            Self::Owl(OwlType::Node(node)) => node.legend(),
            Self::Owl(OwlType::Edge(edge)) => edge.legend(),
            Self::Generic(GenericType::Node(node)) => node.legend(),
            Self::Generic(GenericType::Edge(edge)) => edge.legend(),
            Self::Xsd(XSDType::Node(node)) => node.legend(),
        }
    }
}

impl ElementLegend for GenericNode {
    fn legend(self) -> Option<String> {
        match self {
            Self::Generic => None,
        }
    }
}

impl ElementLegend for GenericEdge {
    fn legend(self) -> Option<String> {
        match self {
            Self::Generic => None,
        }
    }
}

impl ElementLegend for RdfsNode {
    fn legend(self) -> Option<String> {
        match self {
            Self::Class => Some("/node_legends/RdfsClass.png".to_string()),
            Self::Literal => Some("/node_legends/Literal.png".to_string()),
            Self::Resource => Some("/node_legends/RdfsResource.png".to_string()),
            Self::Datatype => Some("/node_legends/Datatype.png".to_string()),
        }
    }
}

impl ElementLegend for RdfsEdge {
    fn legend(self) -> Option<String> {
        match self {
            Self::SubclassOf => Some("/node_legends/SubclassOf.png".to_string()),
        }
    }
}

impl ElementLegend for RdfNode {
    fn legend(self) -> Option<String> {
        match self {
            Self::HTML | Self::PlainLiteral | Self::XMLLiteral => None,
        }
    }
}

impl ElementLegend for RdfEdge {
    fn legend(self) -> Option<String> {
        match self {
            Self::RdfProperty => None,
        }
    }
}

impl ElementLegend for OwlNode {
    fn legend(self) -> Option<String> {
        match self {
            Self::AnonymousClass => Some("/node_legends/AnonymousClass.png".to_string()),
            Self::Class => Some("/node_legends/Class.png".to_string()),
            Self::Complement => Some("/node_legends/Complement.png".to_string()),
            Self::DeprecatedClass => Some("/node_legends/DeprecatedClass.png".to_string()),
            Self::ExternalClass => Some("/node_legends/ExternalClass.png".to_string()),
            Self::EquivalentClass => Some("/node_legends/EquivalentClass.png".to_string()),
            Self::DisjointUnion => Some("/node_legends/DisjointUnion.png".to_string()),
            Self::IntersectionOf => Some("/node_legends/Intersection.png".to_string()),
            Self::Thing => Some("/node_legends/Thing.png".to_string()),
            Self::UnionOf => Some("/node_legends/Union.png".to_string()),
            Self::Real | Self::Rational => None,
        }
    }
}

impl ElementLegend for OwlEdge {
    fn legend(self) -> Option<String> {
        match self {
            Self::DatatypeProperty => Some("/node_legends/DatatypeProperty.png".to_string()),
            Self::DisjointWith => Some("/node_legends/Disjoint.png".to_string()),
            Self::DeprecatedProperty => Some("/node_legends/DeprecatedProperty.png".to_string()),
            Self::ExternalProperty => Some("/node_legends/ExternalProperty.png".to_string()),
            Self::InverseOf => Some("/node_legends/InverseOf.png".to_string()),
            Self::ObjectProperty | Self::ValuesFrom => {
                Some("/node_legends/ObjectProperty.png".to_string())
            }
        }
    }
}

impl ElementLegend for XSDNode {
    fn legend(self) -> Option<String> {
        None
    }
}
