package geotrellis.proj4

import org.osgeo.proj4j._

trait CRS {
  private[proj4] val crs: CoordinateReferenceSystem

  protected def factory = CRS.crsFactory
}

// Common CRS's
object WebMercator extends CRS { lazy val crs = factory.createFromName("EPSG:3857") }
object LatLng extends CRS { lazy val crs = factory.createFromName("EPSG:4326") }

object CRS {
  private val crsFactory = new CRSFactory

  /**
   * Creates a [[CoordinateReferenceSystem]] (CRS) from a well-known name.
   * CRS names are of the form: "<tt>authority:code</tt>", 
   * with the components being: 
   * <ul>
   * <li><b><tt>authority</tt></b> is a code for a namespace supported by
   * PROJ.4.  
   * Currently supported values are 
   * <tt>EPSG</tt>, 
   * <tt>ESRI</tt>, 
   * <tt>WORLD</tt>, 
   * <tt>NA83</tt>, 
   * <tt>NAD27</tt>.
   * If no authority is provided, the <tt>EPSG</tt> namespace is assumed.
   * <li><b><tt>code</tt></b> is the id of a coordinate system in the authority namespace.
   * For example, in the <tt>EPSG</tt> namespace a code is an integer value
   * which identifies a CRS definition in the EPSG database.
   * (Codes are read and handled as strings).
   * </ul>
   * An example of a valid CRS name is <tt>EPSG:3005</tt>.
   * <p>
   * @param name the name of a coordinate system, with optional authority prefix
   * @return the [[CoordinateReferenceSystem]] corresponding to the given name
   */
  def fromName(name: String): CRS = 
    new CRS { val crs = crsFactory.createFromName(name) }

  /**
   * Creates a [[CoordinateReferenceSystem]]
   * from a PROJ.4 projection parameter string.
   * <p>
   * An example of a valid PROJ.4 projection parameter string is:
   * <pre>
   * +proj=aea +lat_1=50 +lat_2=58.5 +lat_0=45 +lon_0=-126 +x_0=1000000 +y_0=0 +ellps=GRS80 +units=m
   * </pre>
   * @param proj4Params a PROJ.4 projection parameter string
   * @return the specified [[CoordinateReferenceSystem]]
   */
  def fromString(proj4Params: String): CRS =
    new CRS { val crs = crsFactory.createFromParameters(null, proj4Params) }

  /**
   * Creates a [[CoordinateReferenceSystem]]
   * from a PROJ.4 projection parameter string.
   * <p>
   * An example of a valid PROJ.4 projection parameter string is:
   * <pre>
   * +proj=aea +lat_1=50 +lat_2=58.5 +lat_0=45 +lon_0=-126 +x_0=1000000 +y_0=0 +ellps=GRS80 +units=m
   * </pre>
   * @param name a name for this coordinate system.
   * @param proj4Params a PROJ.4 projection parameter string
   * @return the specified [[CoordinateReferenceSystem]]
   */
  def fromString(name: String, proj4Params: String): CRS =
    new CRS{ val crs = crsFactory.createFromParameters(name, proj4Params) }
}
